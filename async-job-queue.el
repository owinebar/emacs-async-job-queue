;;; async-job-queue.el --- Dispatch queue of async jobs to a fixed number of slots    -*- lexical-binding: t; -*-

;; Copyright (C) 2023  Onnie Winebarger

;; Author: Onnie Winebarger
;; Copyright (C) 2023 by Onnie Lynn Winebarger <owinebar@gmail.com>
;; Keywords: extensions, lisp
;; Version: 0.1
;; Package-Requires: ((async "1.4") (emacs "25.1"))
;; URL: https://github.com/owinebar/emacs-async-job-queue

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
;; when available, or 1 otherwise

;;; Code:

(require 'cl-lib)
(require 'async)

(defgroup async-job-queue nil
  "Customization group for async-job-queue package."
  :group 'lisp)
  
(defcustom async-job-queue-default-size
  (if (and (>= emacs-major-version 28)
	   (fboundp 'num-processors))
      (num-processors)
    1)
  "Default number of slots in a job queue."
  :type 'natnum
  :group 'async-job-queue)

(defmacro async-job-queue--call-with-warn (&rest app-form)
  "Perform funcall of user callback APP-FORM and display errors as warnings."
  `(condition-case err
       (funcall ,@app-form)
     (error
      (display-warning
       :error
       (format "%s: %S" (car err) (cdr err))))))
    
(cl-defstruct (ajq--table
	       (:constructor async-job-queue--table-create)
	       (:copier async-job-queue--table-copy))
  "Structure describing the table of active jobs and associated lists.
  Slots:
   `id' Symbol for identification in reporting
   `slots' vector of active descriptors
   `active' when non-nil, jobs will be started as soon as a slot is open
            when nil, jobs will simply be pushed onto the queue
   `in-use' number of slots in use
   `free' number of slots free
   `queue' FIFO collection of jobs to be started when a slot is available
   `on-empty' continuation to run when no slots are in use and
              the queue is empty
   `on-activate' hook for functions to call when table is activated
   `on-deactivate' hook for functions to call when table is deactivated
   `first-used' index of the first entry in the list of slots in use
   `last-used' index of the last entry in the list of slots in use
   `first-free' index of first entry in the list of slots not in use
   `last-free'  index of last entry in list of slots free
   `freq'  time between polling in use entries for completed jobs
   `timer' non-nil when jobs are being monitored"
  id
  slots
  (active t)
  in-use
  free
  queue
  on-empty
  on-activate
  on-deactivate
  first-used
  last-used
  first-free
  last-free
  freq
  timer)


(cl-defstruct (ajq--queue
	       (:constructor async-job-queue--queue-create)
	       (:copier async-job-queue--queue-copy))
  head
  last)

(cl-defstruct (ajq--slot
	       (:constructor async-job-queue--slot-create)
	       (:copier async-job-queue--slot-copy))
  "Structure for tracking running jobs.
  Slots:
   `table' job table this slot is contained by
   `index' index of slot in table
   `next' Integer of next entry in list this entry is part of (free or in-use)
   `prev' Integer of previous entry in list this entry is part of
   `job' An async-job-queue--job struct or nil if free"
  table
  index
  next
  prev
  job)


(cl-defstruct (ajq--job
	       (:constructor async-job-queue--job-create)
	       (:copier async-job-queue--job-copy))
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

(defun async-job-queue--job-slot (job)
  "Return the slot for JOB if it is running, nil otherwise."
  (let ((idx (ajq--job-run-slot job)))
    (when idx
      (aref (ajq--table-slots (ajq--job-table job))
	    idx))))

(defun async-job-queue-displayable-table (tbl)
  "Minimal description of job queue TBL."
  `(ajq--table
    (id ,(ajq--table-id tbl))
    (slots ,(length (ajq--table-slots tbl)))
    (active ,(ajq--table-active tbl))
    (in-use ,(ajq--table-in-use tbl)
	    ,(ajq--table-first-used tbl)
	    ,(ajq--table-last-used tbl)
	    ,(ajq--slots-in-use-list tbl))
    (free ,(ajq--table-free tbl)
	  ,(ajq--table-first-free tbl)
	  ,(ajq--table-last-free tbl)
	  ,(ajq--slots-free-list tbl))
    (queue ,(ajq--queue-size (ajq--table-queue tbl)))
    (on-empty ,(and (ajq--table-on-empty tbl) t))
    (freq ,(ajq--table-freq tbl))
    (timer ,(ajq--timer-info (ajq--table-timer tbl)))))

(defun async-job-queue-displayable-slot (slot)
  "Minimal description of queue slot SLOT."
  `(ajq--slot
    (table ,(ajq--table-id (ajq--slot-table slot)))
    (index ,(ajq--slot-index slot))
    (next ,(ajq--slot-next slot))
    (prev ,(ajq--slot-prev slot))
    (job ,(ajq--job-id (ajq--slot-job slot)))))


(defun async-job-queue-displayable-job (job)
  "Minimal description of JOB."
  `(ajq--job
    (id ,(ajq--job-id job))
    (table ,(ajq--table-id (ajq--job-table job)))
    (run-slot ,(ajq--job-run-slot job))
    (started ,(ajq--job-started job))
    (ended ,(ajq--job-ended job))
    (max-time ,(ajq--job-max-time job))
    (future ,(ajq--job-future job))
    (result ,(ajq--job-result job))
    (dispatched ,(and (ajq--job-dispatched job) t))
    (succeed ,(and (ajq--job-succeed job) t))
    (timeout ,(and (ajq--job-timeout job) t))
    (quit ,(and (ajq--job-quit job) t))))

(defun async-job-queue--expr-to-async (e)
  "Convert expression E to thunk as necessary for `async-start'."
  (unless (or (and (consp e)
		   (or (eq (car e) 'lambda)
		       (eq (car e) 'function)))
	      (and (not (symbolp e))
		   (functionp e)))
    (setq e `(lambda () ,e)))
  e)

(defun async-job-queue--make-queue ()
  "Create a simple queue structure."
  (ajq--queue-create :head nil :last nil))

(defun async-job-queue--queue-empty-p (q)
  "Test whether the queue Q can be popped."
  (null (ajq--queue-head q)))

(defun async-job-queue--queue-size (q)
  "Length of queue Q."
  (length (ajq--queue-head q)))

(defun async-job-queue--queue-list (q)
  "Copy of queue Q as list."
  (seq-copy (ajq--queue-head q)))

(defun async-job-queue--queue-push  (q e)
  "Push an element E onto the end of the queue Q."
  (if (ajq--queue-empty-p q)
      (progn
	(setf (ajq--queue-head q) (cons e nil))
	(setf (ajq--queue-last q)
	      (ajq--queue-head q)))
    (let ((l (ajq--queue-last q)))
      (setcdr l (cons e nil))
      (setf (ajq--queue-last q) (cdr l))))
  q)

(defun async-job-queue--queue-pop (q)
  "Pop an element from the front of the queue Q."
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

(defun async-job-queue--slots-in-use-list (tbl)
  "Indexes of slots in in-use list of TBL."
  (let ((slots (ajq--table-slots tbl))
	(idx (ajq--table-first-used tbl))
	used)
    (while idx
      (push idx used)
      (setq idx (ajq--slot-next (aref slots idx))))
    (nreverse used)))

(defun async-job-queue--slots-free-list (tbl)
  "Indexes of slots in free list of TBL."
  (let ((slots (ajq--table-slots tbl))
	(idx (ajq--table-first-free tbl))
	free)
    (while idx
      (push idx free)
      (setq idx (ajq--slot-next (aref slots idx))))
    (nreverse free)))

(defvar async-job-queue--num-tables-created 0
  "Number of job queues created.")

(defun async-job-queue-make-job-queue (freq &optional N on-empty inactive on-activate on-deactivate id)
  "Create an async job queue.
Arguments:
   FREQ - frequency of polling for job completion
   N - number of slots in job queue
   ON-EMPTY - continuation to call when queue is activated and empty
   INACTIVE - if non-nil, create the queue in a deactivated state
   ON-ACTIVATE - hook for functions to call when table is activated
   ON-DEACTIVATE - hook for functions to call when table is deactivated
   ID - A symbol used to identify the queue in `displayable' summaries of
        data structures"
  (unless N
    (setq N async-job-queue-default-size))
  (cl-incf async-job-queue--num-tables-created)
  (unless id
    (setq id (intern (format "async-job-queue-table-%S"
			     async-job-queue--num-tables-created))))
  (let ((tbl (ajq--table-create
	      :id id
	      :slots (make-vector N nil)
	      :active (not inactive)
	      :in-use 0
	      :free N
	      :queue (ajq--make-queue)
	      :on-empty on-empty
	      :on-activate on-activate
	      :on-deactivate on-deactivate
	      :first-used nil
	      :last-used nil
	      :first-free 0
	      :last-free (1- N)
	      :freq freq))
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

;; The slots associated with a table are fixed at table creation
;; This simply moves the first one from the free list to the last
;; entry on the in-use list
(defun async-job-queue--alloc-slot (tbl)
  "Move first free slot of TBL to the end of the in-use list.
Return the allocated slot."
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
    ;; maintain invariant that nil job is free list
    (setf (ajq--slot-job a) t)
    a))

;; Frees an allocated slot
;; returns job from freed slot
(defun async-job-queue--reclaim-slot (s)
  "Move slot S from the in-use list to the free list.
Remove slot S from the in-use list and append
to the free list.  Return the job previously
associated with S"
  (when (null (ajq--slot-job s))
    (signal 'ajq-slot-already-free s))
  (let ((tbl (ajq--slot-table s))
	(prev-used (ajq--slot-prev s))
	(next-used (ajq--slot-next s))
	(job (ajq--slot-job s))
	(idx (ajq--slot-index s))
	first-free last-free slots
	n-in-use n-free
	a b)
    (setf (ajq--slot-job s) nil)
    (setq first-free (ajq--table-first-free tbl)
	  last-free (ajq--table-last-free tbl)
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

(defun async-job-queue--dispatch (tbl &optional job)
  "Dispatch job JOB into first free slot of TBL.
If JOB is nil, dispatch first job from queue of pending jobs."
  (unless job
    (let ((q (ajq--table-queue tbl)))
      (setq job (and (not (ajq--queue-empty-p q))
		     (ajq--queue-pop q)))))
  (when job
    (let ((s (ajq--alloc-slot tbl))
	  (on-dispatch (ajq--job-dispatched job)))
      ;;(message "Allocated slot %S" (ajq--slot-index s))
      (setf (ajq--slot-job s) job)
      ;; (message "Set job for slot %S %S"
      ;; 	       (ajq--slot-index s)
      ;; 	       (ajq--job-id job))
      (setf (ajq--job-run-slot job)
	    (ajq--slot-index s))
      (setf (ajq--job-started job) (current-time))
      (let ((f (async-start
		(ajq--expr-to-async
		 (ajq--job-program job)))))
	(setf (ajq--job-future job) f)
	(when on-dispatch
	  (ajq--call-with-warn on-dispatch job)))))
  job)

(defun async-job-queue-schedule-job (tbl prog &optional id on-dispatch on-finish max-time on-timeout on-quit)
  "Add a job to TBL to run PROG asynchronously.
ID - symbol used in displayable structure summaries
ON-DISPATCH - callback called when job is started
ON-FINISH - continuation to call after job completes normally
MAX-TIME - seconds (wall-clock) to allow job to run before terminating
           as a time-out
ON-TIMEOUT - continuation to call if job is terminated due to timeout
ON-QUIT - continuation to call if job is cancelled"
  (let ((job (ajq--job-create
	      :id id
	      :table tbl
	      :program prog
	      :max-time max-time
	      :dispatched on-dispatch
	      :succeed on-finish
	      :timeout on-timeout
	      :quit on-quit))
	(q (ajq--table-queue tbl))
	(active (ajq--table-active tbl))
	(n-in-use (ajq--table-in-use tbl)))
    (if (or (not active)
	    (not (ajq--queue-empty-p q))
	    (= (ajq--table-free tbl) 0))
	(ajq--queue-push q job)
      ;; optimization
      (ajq--dispatch tbl job))
    ;; timer might not be running if the table was empty
    ;; before this
    (when (and active (= n-in-use 0))
      ;; (message "Ensuring job queue running for job id %S" id)
      (ajq--ensure-queue-running tbl))
    job))

(defun async-job-queue--dispatch-queued (tbl)
  "Dispatch jobs until TBL is full or queue of pending jobs is empty."
  (when (ajq--table-active tbl)
    (let ((q (ajq--table-queue tbl)))
      (while (and (not (ajq--queue-empty-p q))
		  (> (ajq--table-free tbl) 0))
	(ajq--dispatch tbl)))))

(defun async-job-queue--terminate-job-process (job fut)
  "Kill the process associated with JOB running in FUT."
  (condition-case nil
      (delete-process fut)
    (error
     (display-warning
      :warning
      (format "Could not kill process %S for timed-out job %S" fut job)))))
  
(defun async-job-queue--cleanup-job (job slot)
  "Set post-run values for fields of JOB in SLOT."
  (setf (ajq--job-future job) nil)
  (setf (ajq--job-table job) nil)
  (setf (ajq--job-run-slot job) nil)
  (setf (ajq--job-ended job) (current-time))
  (when slot (ajq--reclaim-slot slot)))
  
(defun async-job-queue--handle-finished-job (slot job v)
  "Clean up SLOT in TBL that ran JOB and returned value V."
  (ajq--cleanup-job job slot)
  (setf (ajq--job-result job) (cons v nil))
  (ajq--call-with-warn (ajq--job-succeed job) job v))

(defun async-job-queue--handle-terminated-job (slot job fut)
  "Clean up SLOT in TBL that ran JOB as FUT and timed out."
  (ajq--terminate-job-process job fut)
  (ajq--cleanup-job job slot)
  (setf (ajq--job-result job) nil)
  (ajq--call-with-warn (ajq--job-timeout job) job))

(defun async-job-queue-cancel-job (job)
  "Cancel JOB which may or may not have started running."
  (when (ajq--job-future job)
    (ajq--terminate-job-process job (ajq--job-future job)))
  (ajq--cleanup-job job (ajq--job-slot job))
  (ajq--call-with-warn (ajq--job-quit job) job))

(defun async-job-queue-cancel-job-queue (tbl)
  "Cancel all jobs associated with job queue TBL."
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
      (ajq-cancel-job (ajq--slot-job a))))
  (let ((timer (ajq--table-timer tbl)))
    (when timer
      (cancel-timer timer)
      (setf (ajq--table-timer tbl) nil))))

(defun async-job-queue--timer-info (tmr)
  "Produce non-recursive display of timer TMR."
  (when tmr
    `[timer (triggered ,(timer--triggered tmr))
	    (high-seconds ,(timer--high-seconds tmr))
	    (low-seconds ,(timer--low-seconds tmr))
	    (micro-seconds ,(timer--usecs tmr))
	    (pico-seconds ,(timer--psecs tmr))
	    (repeat-delay ,(timer--repeat-delay tmr))
	    (function ,(timer--function tmr))
	    (idle-delay ,(timer--idle-delay tmr))
	    (integral-multiple
	     ,(if (and (>= emacs-major-version 28)
		       (fboundp 'timer--integral-multiple))
		  (timer--integral-multiple tmr)
		nil))]))

(defun async-job-queue--make-timer (freq rpt fxn &rest args)
  "Make a timer.
FREQ - Initial delay and frequency in seconds or time stamp
RPT - Non-nil to repeat every FREQ
FXN - timer function to call
ARGS - arguments to supply to FXN"
  (if (numberp freq)
      (apply #'run-with-timer freq
	     (and rpt freq) fxn args)
    (apply #'run-at-time freq (and rpt freq) fxn args)))

(defun async-job-queue--ensure-queue-running (tbl)
  "Manage administratvia for TBL.
Ensures queued jobs are run as slots become available, if queue is active.
Sets up a timer if one is needed, cancels an existing one if it is not."
  (let ((on-empty (ajq--table-on-empty tbl))
	(freq (ajq--table-freq tbl))
	(timer (ajq--table-timer tbl))
	(active (ajq--table-active tbl)))
    (when active
      (ajq--dispatch-queued tbl)
      (when (and timer
		 on-empty
		 (= (ajq--table-in-use tbl) 0))
	(ajq--call-with-warn on-empty tbl)))
    (if (= (ajq--table-in-use tbl) 0)
	;; this is correct whether or not the job queue is active
	(when timer
	  (cancel-timer timer)
	  (setq timer nil)
	  (setf (ajq--table-timer tbl) nil))
      (unless (or (not active) timer)
	(setq timer (ajq--make-timer freq freq #'ajq--process-queue tbl))
	(setf (ajq--table-timer tbl) timer)))
    timer))

  
(defun async-job-queue--process-queue (tbl)
  "Check slots of TBL for completed jobs.
Poll running jobs in queue TBL for completion or timeout.
Dispatch pending jobs as slots become available when TBL is active."
  (let ((idx (ajq--table-first-used tbl))
	(slots (ajq--table-slots tbl))
	slot job fut t0 t1 maxt dt)
    (while idx
      ;; (message "Processing slot index %S" idx)
      (setq slot (aref slots idx))
      (setq job (ajq--slot-job slot))
      (setq fut (ajq--job-future job))
      (setq maxt (ajq--job-max-time job))
      (when (async-ready fut)
	;; (message "Future ready %S" fut)
	(ajq--handle-finished-job slot job (async-get fut))
	(setq fut nil))
      (when (and maxt fut)
	;; (message "Future not ready %S checking maxt %S" fut maxt)
	(setq t0 (ajq--job-started job))
	(setq t1 (current-time))
	(setq dt (time-subtract t1 t0))
	(when (<= maxt (float-time dt))
	  (ajq--handle-terminated-job slot job fut)))
      (setq idx (ajq--slot-next slot)))
    (ajq--ensure-queue-running tbl)))

(defun async-job-queue-add-deactivation (tbl callback)
  "Add CALLBACK to on-deactivate hook of job queue TBL"
  (push callback (ajq--table-on-deactivate tbl)))

(defun async-job-queue-add-activation (tbl callback)
  "Add CALLBACK to on-activate hook of job queue TBL"
  (push callback (ajq--table-on-activate tbl)))

(defun async-job-queue-deactivate-queue (tbl &optional key)
  "Deactivate job queue TBL with KEY.
Running jobs will finish.  Queued jobs will remain pending until the
queue is reactivated.  Functions on the deactivation hook will be
called with TBL and KEY as arguments."
  (setf (ajq--table-active tbl) nil)
  (let ((ls (ajq--table-on-deactivate tbl)))
    (while ls
      (funcall (pop ls) tbl key))))

(defun async-job-queue-activate-queue (tbl &optional key)
  "Activate job queue TBL.
Start dispatching jobs and monitoring for job completion."
  (setf (ajq--table-active tbl) t)
  (let ((ls (ajq--table-on-activate tbl)))
    (while ls
      (funcall (pop ls) tbl key)))
  (ajq--ensure-queue-running tbl))


(provide 'async-job-queue)

;;; async-job-queue.el ends here

;; Local Variables:
;; read-symbol-shorthands: (("ajq-" . "async-job-queue-"))
;; End:
