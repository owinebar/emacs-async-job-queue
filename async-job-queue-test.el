;;; async-job-queue-test.el        -*- lexical-binding: t; -*-

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

(let ((q (ajq--make-queue))
      (i 0)
      (N 10))
  (message "Test queue %S" q)
  (while (< i N)
    (ajq--queue-push q i)
    (cl-incf i))
  (message "Test queue pushed %S: %S" N q)
  (while (not (ajq--queue-empty-p q))
    (setq i (ajq--queue-pop q))
    (message "Test queue popped %S: %S" i q))
  (message "Queue test done: %S" q)
  nil)
    
  
(cl-defstruct (ajqt--test
	       (:constructor ajq--test-create)
	       (:copier ajq--test-copy))
  "Test descriptor"
  lim0
  lim1
  N
  exprs)
  
(cl-defstruct (ajqt--test-run
	       (:constructor ajq--test-run-create)
	       (:copier ajq--test-run-copy))
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

  
(defun ajqt--make-test-expr (id &optional lim0 lim1)
  (unless lim0
    (setq lim0 10))
  (unless lim1
    (setq lim1 60))
  `(progn (sleep-for ,(+ (random (- lim1 lim0)) lim0)) '(,id ,lim1)))

(defun ajqt--make-test (N &optional lim0 lim1)
  (unless lim0
    (setq lim0 10))
  (unless lim1
    (setq lim1 60))
  (let ((exprs nil)
	(i N))
    (while (> i 0)
      (push `(,i ,(ajqt--make-test-expr i lim0 lim1)) exprs))
    (ajq--test-create
     :lim0 lim0
     :lim1 lim1
     :N N
     :exprs exprs)))

(defun ajqt--report-table (log tbl)
  (with-current-buffer log
    (let ((q (ajq--table-queue tbl)))
      (insert (format "Current table free %S"
		      (ajq--table-free tbl)))
      (insert (format "Current table in-use %S"
		      (ajq--table-in-use tbl)))
      (insert (format "Current table queue-size %S"
		      (ajq--queue-size q)))
      (insert (format "Current table slots\n%S"
		      (ajq--table-slots tbl))))))

(defun ajqt--dispatched-test (job test-run)
  (let ((q (ajqt--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (ajq--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (ajqt--test-run-log test-run)))
    (with-current-buffer lb
      (insert (format "Dispatched id %S %S" id job))
      (ajqt--report-table tbl))
    (ajq--queue-push q `(Dispatched ,id ,t0 ,t1 ,job))))

(defun ajqt--succeed-test (job v test-run)
  (let ((q (ajqt--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (ajq--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (ajqt--test-run-log test-run))
	dt)
    (setq dt (float-time (time-subtract t1 t0))) 
    (with-current-buffer lb
      (insert (format "Success id %S %S %S" id v job))
      (ajqt--report-table lb tbl))
    (ajq--queue-push q `(Success ,id ,t0 ,t1 ,dt ,v ,job))))

(defun ajqt--timeout-test (job test-run)
  (let ((q (ajqt--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (ajq--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (ajqt--test-run-log test-run))
	dt)
    (setq dt (float-time (time-subtract t1 t0)))
    (with-current-buffer lb
      (insert (format "Timeout id %S %S %S" dt id job))
      (ajqt--report-table lb tbl))
    (ajq--queue-push q `(Timeout ,id ,t0 ,t1 ,dt ,v ,job))))

(defun ajqt--quit-test (job test-run)
  (let ((q (ajqt--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (ajq--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (ajqt--test-run-log test-run))
	dt)
    (setq dt (float-time (time-subtract t1 t0)))
    (with-current-buffer lb
      (insert (format "Quit id %S %S %S" dt id job))
      (ajqt--report-table tbl))
    (ajq--queue-push q `(quit ,id ,t0 ,t1 ,dt ,v ,job))))

(defvar ajqt--tests-created 0)

(defun ajqt--make-test-run (tbl test &optional freq max-time d logname)
  (cl-incf ajqt--tests-created)
  (unless id
    (setq id (intern (format "ajq-test-%S" ajqt--tests-created))))
  (unless logname
    (setq logname (format "*%s-log*" id)))
  (unless freq
    (setq freq 1))
  (setf (ajq--table-freq tbl) freq)
  (setf (ajq--table-max-time tbl) max-time)
  (let ((tr (ajqt--test-run-create
	     :id id
	     :table tbl
	     :freq freq
	     :max-time max-time
	     :test test
	     :results (ajq--make-queue)
	     :log (get-buffer-create logname))))
    (setf (ajqt--test-run-on-succeed tr)
	  (lambda (job v)
	    (ajqt--succeed-test job v tr)))
    (setf (ajqt--test-run-on-timeout tr)
	  (lambda (job v)
	    (ajqt--timeout-test job tr)))
    (setf (ajqt--test-run-on-quit tr)
	  (lambda (job v)
	    (ajqt--quit-test job tr)))
    tr))

(defun ajqt-run-test (test freq sz &optional id max-time logname)
  (let ((tbl (ajq-make-job-queue freq sz))
	tr ls e job jobs)
    (setq tr (ajqt--make-test-run tbl test freq max-time id logname))
    (setf (ajq--table-on-empty tbl)
	  (lambda (tbl)
	    (let ((log (ajqt--test-run-log tr)))
	      (with-current-buffer log
		(insert (format "Completed %S \n%S\n%S\n" id tbl tr))))))
    (setq ls (ajqt--test-exprs test))
    (while ls
      (setq e (pop ls)
	    job (ajq-schedule-job
		 tbl
		 (cadr e)
		 (car e)
		 (ajqt--test-run-on-dispatch tr)
		 (ajqt--test-run-on-succeed tr)
		 (ajqt--test-run-max-time tr)
		 (ajqt--test-run-on-timeout tr)
		 (ajqt--test-run-on-quit tr)))
      (push job jobs))
    jobs))
      

;;; test queue

(provide 'async-job-queue-test)
;;; async-job-queue-test.el ends here

;; Local Variables:
;; read-symbol-shorthands: (("ajq-" . "async-job-queue-")("ajqt-" . "async-job-queue-test-"))
;; End:
