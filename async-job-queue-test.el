;;; async-job-queue-test.el --- test support for async-job-queue   -*- lexical-binding: t; -*-

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

;; Test infrastructure for async-job-queue
;; Create job queues of programs that sleep for random amount of time
;; 

;;; Code:

(require 'async-job-queue)
(require 'cl-lib)


;; test of ajq--queue struct

;; (let ((q (ajq--make-queue))
;;       (i 0)
;;       (N 10))
;;   (message "Test queue %S" q)
;;   (while (< i N)
;;     (ajq--queue-push q i)
;;     (cl-incf i))
;;   (message "Test queue pushed %S: %S" N q)
;;   (while (not (ajq--queue-empty-p q))
;;     (setq i (ajq--queue-pop q))
;;     (message "Test queue popped %S: %S" i q))
;;   (message "Queue test done: %S" q)
;;   nil)
    
  
(cl-defstruct (async-job-queue-test--test
	       (:constructor ajqt--test-create)
	       (:copier ajqt--test-copy))
  "Test descriptor"
  lim0
  lim1
  N
  exprs)
  
(cl-defstruct (async-job-queue-test--test-run
	       (:constructor async-job-queue-test--test-run-create)
	       (:copier async-job-queue-test--test-run-copy))
  "Test run"
  id
  table
  test
  results
  log
  freq
  max-time
  on-dispatch
  on-succeed
  on-timeout
  on-quit)

  
(defun async-job-queue-test--make-test-expr (id &optional lim0 lim1)
  "Make a test expression labeled ID that sleeps between LIM0 and LIM1 seconds."
  (unless lim0
    (setq lim0 10))
  (unless lim1
    (setq lim1 60))
  (let (p e)
    (if (<= lim1 lim0)
	(setq e `(progn (sleep-for ,lim1) '(,id ,lim1)))
      (setq p (+ (random (- lim1 lim0)) lim0)
	    e `(progn
		 (sleep-for ,p)
		 '(,id ,p))))
    ;; (message "Test expr %S %S %S: %S"
    ;; 	     id
    ;; 	     lim0
    ;; 	     lim1
    ;; 	     e)
    e))

(defun async-job-queue-test--make-test (N &optional lim0 lim1)
  "Make a test of N jobs that sleep between LIM0 and LIM1 seconds."
  (unless lim0
    (setq lim0 10))
  (unless lim1
    (setq lim1 60))
  (let ((exprs nil)
	(i N))
    (while (> i 0)
      (push `(,i ,(async-job-queue-test--make-test-expr i lim0 lim1)) exprs)
      (message "%S %S" i exprs)
      (cl-decf i))
    (ajqt--test-create
     :lim0 lim0
     :lim1 lim1
     :N N
     :exprs exprs)))

(defun async-job-queue-test--report-table (log tbl)
  "Report the state of TBL in buffer LOG."
  (with-current-buffer log
    (goto-char (point-max))
    (let ((q (ajq--table-queue tbl)))
      (insert (format "Current table %S free %S\n"
		      (ajq--table-free tbl)
		      (ajq--slots-free-list tbl)))
      (insert (format "Current table %S in-use %S\n"
		      (ajq--table-in-use tbl)
		      (ajq--slots-in-use-list tbl)))
      (insert (format "Current table queue-size %S\n"
		      (queue-length q))))))

(defun async-job-queue-test--dispatched-test (job test-run)
  "Report dispatch of test expression in JOB for TEST-RUN."
  (message "dispatched-test %S %S"
	   (async-job-queue-displayable-job job)
	   (async-job-queue-test-displayable-test-run test-run))
  (let ((q (async-job-queue-test--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (async-job-queue-test--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (async-job-queue-test--test-run-log test-run)))
    (with-current-buffer lb
      (goto-char (point-max))
      (insert (format "Dispatched job id %S\n" id))
      (async-job-queue-test--report-table lb tbl))
    (queue-enqueue q `(Dispatched ,id ,t0 ,t1))
    (message "Completed dispatched-test %S" id)))

(defun async-job-queue-test--succeed-test (job v test-run)
  "Report completion of test expression in JOB for TEST-RUN with result V."
  (message "succeed-test %S %S %S"
	   v
	   (async-job-queue-displayable-job job)
	   (async-job-queue-test-displayable-test-run test-run))
  (let ((q (async-job-queue-test--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (async-job-queue-test--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (async-job-queue-test--test-run-log test-run))
	dt)
    (setq dt (float-time (time-subtract t1 t0)))
    (with-current-buffer lb
      (goto-char (point-max))
      (insert (format "Success job id %S returned %S\n" id v))
      (async-job-queue-test--report-table lb tbl))
    (queue-enqueue q `(Success ,id ,t0 ,t1 ,dt ,v))
    (message "Completed succeed-test %S" v)))

(defun async-job-queue-test--timeout-test (job test-run)
  "Report timeout of test expression in JOB for TEST-RUN."
  (message "timeout-test %S %S"
	   (async-job-queue-displayable-job job)
	   (async-job-queue-test-displayable-test-run test-run))
  (let ((q (async-job-queue-test--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (async-job-queue-test--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (async-job-queue-test--test-run-log test-run))
	dt)
    (setq dt (float-time (time-subtract t1 t0)))
    (with-current-buffer lb
      (goto-char (point-max))
      (insert (format "Timeout job %S after %S\n" id dt))
      (async-job-queue-test--report-table lb tbl))
    (queue-enqueue q `(Timeout ,id ,t0 ,t1 ,dt))))

(defun async-job-queue-test--quit-test (job test-run)
  "Report cancellation of test expression in JOB for TEST-RUN."
  (message "quit-test %S %S"
	   (async-job-queue-displayable-job job)
	   (async-job-queue-test-displayable-test-run test-run))
  (let ((q (async-job-queue-test--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (async-job-queue-test--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (async-job-queue-test--test-run-log test-run))
	dt)
    (setq dt (float-time (time-subtract t1 t0)))
    (with-current-buffer lb
      (goto-char (point-max))
      (insert (format "Quit job id %S after %S\n" id dt))
      (async-job-queue-test--report-table lb tbl))
    (queue-enqueue q `(quit ,id ,t0 ,t1 ,dt))))

(defvar async-job-queue-test--tests-created 0
  "Number of test runs created.")

(defun async-job-queue-test--make-test-run (tbl test &optional freq max-time id logname)
  "Make a test run for TEST with job queue TBL.
Arguments:
  TBL - job queue to test
  TEST - structure defining the test
  FREQ - polling frequency for TBL
  MAX-TIME - the timeout for each test expression
  ID - identifier of test for reporting in log buffer
  LOGNAME - name of buffer to use for logging"
  (cl-incf async-job-queue-test--tests-created)
  (unless id
    (setq id (intern (format "ajq-test-%S" async-job-queue-test--tests-created))))
  (unless logname
    (setq logname (format "*%s-log*" id)))
  (unless freq
    (setq freq 1))
  (setf (ajq--table-freq tbl) freq)
  (let ((tr (async-job-queue-test--test-run-create
	     :id id
	     :table tbl
	     :freq freq
	     :max-time max-time
	     :test test
	     :results (make-queue)
	     :log (get-buffer-create logname))))
    (setf (async-job-queue-test--test-run-on-dispatch tr)
	  (lambda (job)
	    (async-job-queue-test--dispatched-test job tr)))
    (setf (async-job-queue-test--test-run-on-succeed tr)
	  (lambda (job v)
	    (async-job-queue-test--succeed-test job v tr)))
    (setf (async-job-queue-test--test-run-on-timeout tr)
	  (lambda (job)
	    (async-job-queue-test--timeout-test job tr)))
    (setf (async-job-queue-test--test-run-on-quit tr)
	  (lambda (job)
	    (async-job-queue-test--quit-test job tr)))
    tr))

(defun async-job-queue-test-run-test (test freq sz &optional id max-time logname)
  "Run TEST with frequency FREQ and SZ slots.
Arguments:
  TEST -
  FREQ - polling frequency
  SZ - number of slots in table
  ID - identifier of test for reporting
  MAX-TIME - timeout limit for jobs in test
  LOGNAME - name of buffer to use for reporting"
  (let ((tbl (ajq-make-job-queue freq sz))
	tr ls e job jobs)
    (setq tr (async-job-queue-test--make-test-run tbl test freq max-time id logname))
    (setf (ajq--table-on-empty tbl)
	  (lambda (tbl)
	    (let ((log (async-job-queue-test--test-run-log tr)))
	      (with-current-buffer log
		(insert (format "Completed test run %S on queue %S\n" id (ajq--table-id tbl)))
		(let ((inhibit-message t))
		  (pp (async-job-queue-test-displayable-test-run tr) (current-buffer)))))))
    (setq ls (async-job-queue-test--test-exprs test))
    (while ls
      (setq e (pop ls)
	    job (ajq-schedule-job
		 tbl
		 (cadr e)
		 (car e)
		 (async-job-queue-test--test-run-on-dispatch tr)
		 (async-job-queue-test--test-run-on-succeed tr)
		 (async-job-queue-test--test-run-max-time tr)
		 (async-job-queue-test--test-run-on-timeout tr)
		 (async-job-queue-test--test-run-on-quit tr)))
      (push job jobs))
    `(,tbl ,tr ,jobs)))

(defun async-job-queue-test-displayable-test-run (tr)
  "Produce a non-recursive display for the data in test run TR."
  `(async-job-queue-test-test-run
    (id ,(async-job-queue-test--test-run-id tr))
    (table ,(ajq--table-id (async-job-queue-test--test-run-table tr)))
    (test ,(async-job-queue-test--test-run-test tr))
    (results ,(async-job-queue-test--test-run-results tr))
    (log ,(async-job-queue-test--test-run-log tr))
    (freq ,(async-job-queue-test--test-run-freq tr))
    (max-time ,(async-job-queue-test--test-run-max-time tr))
    (on-dispatch ,(and (async-job-queue-test--test-run-on-dispatch tr) t))
    (on-succeed ,(and (async-job-queue-test--test-run-on-succeed tr) t))
    (on-timeout ,(and (async-job-queue-test--test-run-on-timeout tr) t))
    (on-quit ,(and (async-job-queue-test--test-run-on-quit tr) t))))


;;; test queue

(defvar async-job-queue-test-test1
  (async-job-queue-test--make-test 10 20 20)
  "Simple test definition 1.")

(defvar async-job-queue-test-test2
  (async-job-queue-test--make-test 5 20 30)
  "Simple test definition 2.")

(defvar async-job-queue-test-tbl
  (ajq-make-job-queue 1 2)
  "Job queue with 2 slots and 1s polling frequency.")

(defvar async-job-queue-test--slot1 (ajq--alloc-slot async-job-queue-test-tbl)
  "Test slot allocation function.")
(defvar async-job-queue-test--job1 (ajq--reclaim-slot ajqt--slot1)
  "Test slot reclamation function.")

(defvar async-job-queue-test-test1-run1
  (async-job-queue-test--make-test-run
   async-job-queue-test-tbl async-job-queue-test-test1
   1 nil 't1)
  "Create a test run specification for test definition 1.")

(defvar async-job-queue-test-test1-run2
  (async-job-queue-test-run-test async-job-queue-test-test1 10 2 't1r5))

(defvar async-job-queue-test-test1-run3
  (async-job-queue-test-run-test async-job-queue-test-test1 1 (num-processors) 't1r3))


;;(pp (car async-job-queue-test-test1-run2) (get-buffer "*scratch*"))

;;(cancel-timer (ajq--table-timer (car async-job-queue-test-test1-run2)))

(provide 'async-job-queue-test)
;;; async-job-queue-test.el ends here

;; Local Variables:
;; read-symbol-shorthands: (("ajq-" . "async-job-queue-")("ajqt-" . "async-job-queue-test-"))
;; End:
