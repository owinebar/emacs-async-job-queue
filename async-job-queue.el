;;; async-job-queue.el        -*- lexical-binding: t; -*-

;; Copyright (C) 2023  Onnie Winebarger

;; Author: Onnie Winebarger
;; Copyright (C) 2023 by Onnie Lynn Winebarger <owinebar@gmail.com>
;; Keywords: extensions, lisp

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Provide a queue for dispatching jobs asynchronously
;; while maintaining a fixed maximum number of active jobs
;;
;; Lisp code can use this to create an arbitrary number of jobs
;; to be run asynchronously without immediately starting those
;; jobs.  The default number of jobs allowed to run simultaneously
;; is set to the number of processors reported by (num-processors)

;;; Code:

(require 'cl-lib)
(require 'async)

(defgroup async-job-queue nil
  "Customization group for async-job-queue package"
  :group 'lisp)
  
(defcustom ajq--default-size (num-processors)
  "Default for maximum number of processes to use for
asynchronous execution of queued jobs"
  :type 'natnum
  :group 'async-job-queue)

(defcustom ajq--default-freq 1
  "Default polling frequency for job queues, in seconds (number) or
relative time string"
  :type '(choice (number :tag "seconds") (string :tag "relative time"))
  :group 'async-job-queue)


(cl-defstruct (ajq--table 
	       (:constructor ajq--table-create)
	       (:copier ajq--table-copy))
  "Structure describing the table of active jobs and associated lists
  Slots:
   `slots' vector of active descriptors
   `in-use' number of slots in use
   `free' number of slots free
   `queue' FIFO collection of jobs to be started when a slot is available
   `on-empty' continuation to run when no slots are in use and
              the queue is empty
   `first-used' index of the first entry in the list of slots in use
   `last-used' index of the last entry in the list of slots in use
   `first-free' index of first entry in the list of slots not in use
   `last-free'  index of last entry in list of slots free
   `poll-freq'  time between polling in use entries for completed jobs
   `timer' non-nil when jobs are being monitored"
  slots
  in-use
  free
  queue
  on-empty
  first-used
  last-used
  first-free
  last-free
  poll-freq
  timer)

(cl-defstruct (ajq--queue
	       (:constructor ajq--queue-create)
	       (:copier ajq--queue-copy))
  head
  last)

(cl-defstruct (ajq--slot
	       (:constructor ajq--slot-create)
	       (:copier ajq--slot-copy))
  "Structure for tracking running jobs
  Slots:
   `table' job table this slot is contained by
   `index' index of slot in table
   `next' Integer of next entry in list this entry is part of (free or in-use)
   `prev' Integer of previous entry in list this entry is part of
   `job' An ajq--job struct or nil if free"
  table
  index
  next
  prev
  job)

(cl-defstruct (ajq--job 
	       (:constructor ajq--job-create)
	       (:copier ajq--job-copy))
  "Structure for jobs unboxed has started.  Callbacks take job struct as first argument.
  Slots:
   `id' Identifier
   `table' job-queue structure managing this job
   `run-slot' when running, index of slot in active table, else nil
   `program' Elisp program run in the job
   `started' start time
   `max-time' maximum wall clock time to allow run, nil for no limit
   `future' object representing the async process
   `ended' time result was ready or job terminated
   `result' nil if timed out, else singleton list holding return value
   `dispatched' callback function to run immediately after starting the job
   `succeed' callback function to use when results are available
   `timeout' callback function to use when process times out
   `quit' callback function to use when job is canceled"
  id
  table
  run-slot
  program
  started
  max-time
  future
  ended
  result
  dispatched
  succeed
  timeout
  quit)


(defun ajq--make-queue ()
  "Create a simple queue structure"
  (ajq--queue-create :head nil :last nil))

(defun ajq--queue-empty-p (q)
  "Test queue whether the queue can be popped"
  (null (ajq--queue-head q)))

(defun ajq--queue-size (q)
  "Length of queue"
  (length (ajq--queue-head q)))

(defun ajq--queue-list (q)
  "Copy of queue as list"
  (seq-copy (ajq--queue-head q)))

(defun ajq--queue-push  (q e)
  "Push an element onto the end of the queue"
  (if (ajq--queue-empty-p q)
      (progn
	(setf (ajq--queue-head q) (cons e nil))
	(setf (ajq--queue-last q)
	      (ajq--queue-head q)))
    (let ((l (ajq--queue-last q)))
      (setcdr l (cons e nil))
      (setf (ajq--queue-last q) (cdr l))))
  q)

(defun ajq--queue-pop (q)
  "Pop an element from the front of the queue"
  (let ((h (ajq--queue-head q))
	(l (ajq--queue-last q))
	r)
    (setq r (car h))
    (if (eq h l)
	;; (cdr l) is always nil
	(progn
	  (setf (ajq--queue-head q) nil)
	  (setf (ajq--queue-last q) nil))
      (setf (ajq--queue-head q) (cdr h)))
    r))
  
(defun ajq-make-job-queue (freq &optional N on-empty)
  "Creates an async job queue and all the structures that will be allocated
for tracking the processor slots that can be used"
  (unless N
    (setq N ajq--default-size))
  (let ((tbl (ajq--table-create 
	      :slots (make-vector N)
	      :in-use 0
	      :free N
	      :queue (ajq--make-queue)
	      :on-empty on-empty
	      :first-used nil
	      :last-used nil
	      :first-free 0
	      :last-free (1- N)
	      :poll-freq freq))
	(i 0)
	(prev nil)
	(next 1)
	slots a)
    (setq slots (ajq--table-slots tbl))
    (while (< i N)
      ;; note the nil job indicates the entry is on the free list
      ;; a non-nil job indicates the entry is on the in-use list
      (setq a (ajq--slot-create
	       :table tbl
	       :index i
	       :next next
	       :prev prev
	       :job nil)
	    prev i
	    i next)
      (cl-incf next)
      (aset slots prev a))
    (setf (ajq--slot-next a) nil)
    tbl))

(defun ajq-set-slot-job (at k job)
  (setf (ajq--slot-job
	 (aref (ajq--table-slots at) k))
	job))

;; The slots associated with a table are fixed at table creation
;; This simply moves the first one from the free list to the last
;; entry on the in-use list
(defun ajq--alloc-slot (tbl)
  "Move the first free slot onto the end of the
in-use list.  Returns the allocated slot"
  (when (= (ajq--table-free tbl) 0)
    (signal 'ajq-table-no-free-slot tbl))
  (let ((first-free (ajq--table-first-free tbl))
	(last-used  (ajq--table-last-used tbl))
	(slots (ajq--table-slots tbl))
	(n-in-use (ajq--table-in-use tbl))
	(n-free (ajq--table-free tbl))
	next-free a b)
    (setq a (aref slots first-free)
	  next-free (ajq--slot-next a))
    (setf (ajq--table-first-free tbl) next-free)
    (when next-free
      (setq b (aref slots next-free))
      ;; this is first free now
      (setf (ajq--slot-prev b) nil))
    ;; a will be last used
    (setf (ajq--slot-next a) nil)
    ;; prev field of a is nil by definition of first free
    (setf (ajq--slot-prev a) last-used)
    (unless next-free
      (setf (ajq--table-last-free  tbl) nil))
    (setf (ajq--table-free tbl) (1- n-free))
    (setf (ajq--table-in-use tbl) (1+ n-in-use))
    (unless last-used
      (setf (ajq--table-first-used tbl) first-free))
    (when last-used
      (setq b (aref slots last-used))
      ;; next field of b is nil by definition of last used
      (setf (ajq--slot-next b) first-free))
    (setf (ajq--table-last-used tbl) first-free)
    a))

;; Frees an allocated slot
;; returns job from freed slot
(defun ajq--reclaim-slot (s)
  "Move the first free slot onto the end of the
in-use list.  Returns the allocated slot"
  (when (null (ajq--slot-job s))
    (signal 'ajq-slot-already-free s))
  (let ((tbl (ajq--slot-table s))
	(prev-used (ajq--slot-prev s))
	(next-used (ajq--slot-next s))
	(job (ajq--slot-job s))
	(idx (ajq--slot-index s))
	last-free slots n-in-use n-free
	next-free a b)
    (setf (ajq--slot-job s) nil)
    (setq last-free (ajq--table-last-free tbl)
	  slots (ajq--table-slots tbl)
	  n-in-use (ajq--table-in-use tbl)
	  n-free (ajq--table-free tbl))
    ;; remove s from the in-use list
    (unless prev-used
      (setf (ajq--table-first-used tbl) next-used))
    (unless next-used
      (setf (ajq--table-last-used tbl) prev-used))
    (when prev-used
      (setq a (aref slots prev-used)))
    (when next-used
      (setq b (aref slots next-used)))
    (when a
      (setf (ajq--slot-next a) next-used))
    (when b
      (setf (ajq--slot-prev b) prev-used))
    (setf (ajq--table-in-use tbl) (1- n-in-use))
    (setf (ajq--table-free tbl) (1+ n-free))
    (unless first-free
      (setf (ajq--table-first-free tbl) idx))
    (when last-free
      (setq a (aref slots last-free))
      (setf (ajq--slot-next a) idx))
    (setf (ajq--table-last-free tbl) idx)
    job))

(defun ajq--dispatch (tbl &optional job)
  "Internal routine that starts a job"
  (unless job
    (let ((q (ajq--table-queue tbl)))
      (setq job (and (not (ajq--queue-empty-p q))
		     (ajq--queue-pop q)))))
  (when job
    (let ((s (ajq--alloc-slot tbl))
	  (on-dispatch (ajq--job-dispatched job)))
      (setf (ajq--job-run-slot job) slot-idx)
      (setf (ajq--job-started job) (current-time))
      (let ((f (async-start (ajq--job-program job))))
	(setf (ajq--job-future job) f)
	(when on-dispatch
	  (funcall on-dispatch job)))))
  job)
  
(defun ajq-schedule-job (tbl prog &optional id on-dispatch on-finish max-time on-timeout on-quit)
  "User function to add a job to the job queue"
  (let ((job (ajq--job-create
	      :id id
	      :table tbl
	      :program prog
	      :max-time max-time
	      :dispatched on-dispatch
	      :succeed on-finish
	      :timeout on-timeout
	      :quit on-quit))
	(q (ajq--table-queue tbl)))
    (if (or (not (ajq--queue-empty-p q))
	    (= (ajq--table-free tbl) 0))
	(ajq--queue-push q job)
      (ajq--dispatch tbl job))
    job))

(defun ajq--dispatch-queued (tbl)
  "Internal routine to fill the free job slots with queued jobs"
  (let ((q (ajq--table-queue tbl))
	job)
    (while (and (not (ajq-queue-empty-p q))
		(> (ajq--table-free tbl) 0))
      (setq job (ajq--queue-pop q))
      (ajq--dispatch tbl job))))

(defun ajq--terminate-job-process (job fut)
  "Kills the process associated with a job"
  (condition-case nil
      (delete-process fut)
    (error
     (display-warning
      :warning
      (format "Could not kill process %S for timed-out job %S" fut job)))))
  
(defun ajq--cleanup-job (job slot)
  "Set post-run values for fields of job struct"
  (setf (ajq--job-future job) nil)
  (setf (ajq--job-table job) nil)
  (setf (ajq--job-run-slot job) nil)
  (setf (ajq-job-ended job) (current-time))
  (ajq--reclaim-slot slot))
  
(defun ajq--handle-finished-job (tbl slot job v)
  "Clean up slot and job after normal job completion"
  (ajq--cleanup-job job slot)
  (setf (ajq--job-result job) (cons v nil))
  (funcall (ajq--job-succeed job) job v))

(defun ajq--handle-terminated-job (tbl slot job fut)
  "Clean up slot and job after timeout"
  (ajq--terminate-job-process job fut)
  (ajq--cleanup-job job slot)
  (setf (ajq--job-result job) nil)
  (funcall (ajq--job-timeout job) job))

(defun ajq-cancel-job (job)
  "Cancel a job which may or may not have started running"
  (when (ajq--job-future job)
    (ajq--terminate-job-process job (ajq--job-future job)))
  (ajq--cleanup-job job slot)
  (funcall (ajq--job-quit job) job))

(defun ajq-cancel-job-queue (tbl)
  "Cancel all jobs associated with a job queue"
  ;; first the queued jobs, if any
  (let ((q (ajq--table-queue tbl)))
    (while (not (ajq--queue-empty-p q))
      (ajq-cancel-job (ajq--queue-pop q))))
  (let ((idx (ajq--table-first-used tbl))
	(slots (ajq--table-slots tbl))
	a)
    (while idx
      (setq a (aref slots idx)
	    idx (ajq--slot-next a))
      (ajq-cancel-job (ajq-slot-job a))))
  (let ((timer (ajq--table-timer tbl)))
    (when timer
      (cancel-timer timer))))

(defun ajq--ensure-queue-running (tbl)
  "Manages administratvia required to ensure queued jobs are run
as slots become available"
  (let ((idx (ajq--table-first-used tbl))
	(slots (ajq--table-slots tbl))
	(on-empty (ajq--table-on-empty tbl))
	(freq (ajq--table-poll-freq tbl))
	(timer (ajq--table-timer tbl)))
    (ajq--dispatch-queued tbl)
    (when (and timer on-empty (= (ajq--table-in-use tbl) 0))
      (funcall on-empty tbl))
    (if (= (ajq--table-in-use tbl) 0)
	(when timer
	  (cancel-timer timer))
      (unless timer
	(setf (ajq--table-timer tbl)
	      (if (numberp freq)
		  (run-with-timer freq freq
				  #'ajq--process-queue
				  tbl)
		  (run-at-time freq freq
			       #'ajq--process-queue
			       tbl)))))))
  
(defun ajq--process-queue (tbl)
  "The main routine for checking and handling job completion and timeouts"
  (let ((idx (ajq--table-first-used tbl))
	(slots (ajq--table-slots tbl))
	(on-empty (ajq--table-on-empty tbl))
	(freq (ajq--table-poll-freq tbl))
	slot job fut v t0 t1 maxt dt)
    (while idx
      (setq slot (aref slots idx))
      (setq job (ajq--slot-job slot))
      (setq fut (ajq--job-future job))
      (setq maxt (ajq-job-max-time job))
      (when (async-ready fut)
	(ajq--handle-finished-job tbl slot job (async-get fut))
	(setq fut nil))
      (when (and maxt fut)
	(setq t0 (ajq-job-started job))
	(setq t1 (current-time))
	(setq dt (time-subtract t1 t0))
	(when (<= maxt (float-time dt))
	  (ajq--handle-terminated-job tbl slot job fut))
	(setq idx (ajq--slot-next slot))))
    (ajq--ensure-queue-running tbl)))




(provide 'async-job-queue)

;;; async-job-queue.el ends here

;; Local Variables:
;; read-symbol-shorthands: (("ajq-" . "async-job-queue-"))
;; End:

