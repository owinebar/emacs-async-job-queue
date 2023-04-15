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
    
  
(cl-defstruct (ajqt--test
	       (:constructor ajq--test-create)
	       (:copier ajq--test-copy))
  "Test descriptor"
  lim0
  lim1
  N
  exprs)
  
(cl-defstruct (ajqt--test-run
	       (:constructor ajqt--test-run-create)
	       (:copier ajqt--test-run-copy))
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

(defun ajqt--make-test (N &optional lim0 lim1)
  (unless lim0
    (setq lim0 10))
  (unless lim1
    (setq lim1 60))
  (let ((exprs nil)
	(i N))
    (while (> i 0)
      (push `(,i ,(ajqt--make-test-expr i lim0 lim1)) exprs)
      (message "%S %S" i exprs)
      (cl-decf i))
    (ajq--test-create
     :lim0 lim0
     :lim1 lim1
     :N N
     :exprs exprs)))

(defun ajqt--report-table (log tbl)
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
		      (ajq--queue-size q))))))

(defun ajqt--dispatched-test (job test-run)
  (let ((q (ajqt--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (ajqt--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (ajqt--test-run-log test-run)))
    (with-current-buffer lb
      (goto-char (point-max))
      (insert (format "Dispatched job id %S\n" id))
      (ajqt--report-table lb tbl))
    (ajq--queue-push q `(Dispatched ,id ,t0 ,t1))))

(defun ajqt--succeed-test (job v test-run)
  (let ((q (ajqt--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (ajqt--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (ajqt--test-run-log test-run))
	dt)
    (setq dt (float-time (time-subtract t1 t0))) 
    (with-current-buffer lb
      (goto-char (point-max))
      (insert (format "Success job id %S returned %S\n" id v))
      (ajqt--report-table lb tbl))
    (ajq--queue-push q `(Success ,id ,t0 ,t1 ,dt ,v))))

(defun ajqt--timeout-test (job test-run)
  (let ((q (ajqt--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (ajqt--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (ajqt--test-run-log test-run))
	dt)
    (setq dt (float-time (time-subtract t1 t0)))
    (with-current-buffer lb
      (goto-char (point-max))
      (insert (format "Timeout job %S after %S\n" id dt))
      (ajqt--report-table lb tbl))
    (ajq--queue-push q `(Timeout ,id ,t0 ,t1 ,dt))))

(defun ajqt--quit-test (job test-run)
  (let ((q (ajqt--test-run-results test-run))
	(t0 (ajq--job-started job))
	(t1 (ajq--job-ended job))
	(tbl (ajqt--test-run-table test-run))
	(id (ajq--job-id job))
	(lb (ajqt--test-run-log test-run))
	dt)
    (setq dt (float-time (time-subtract t1 t0)))
    (with-current-buffer lb
      (goto-char (point-max))
      (insert (format "Quit job id %S after %S\n" id dt))
      (ajqt--report-table lb tbl))
    (ajq--queue-push q `(quit ,id ,t0 ,t1 ,dt ,v))))

(defvar ajqt--tests-created 0)

(defun ajqt--make-test-run (tbl test &optional freq max-time id logname)
  (cl-incf ajqt--tests-created)
  (unless id
    (setq id (intern (format "ajq-test-%S" ajqt--tests-created))))
  (unless logname
    (setq logname (format "*%s-log*" id)))
  (unless freq
    (setq freq 1))
  (setf (ajq--table-freq tbl) freq)
  (let ((tr (ajqt--test-run-create
	     :id id
	     :table tbl
	     :freq freq
	     :max-time max-time
	     :test test
	     :results (ajq--make-queue)
	     :log (get-buffer-create logname))))
    (setf (ajqt--test-run-on-dispatch tr)
	  (lambda (job)
	    (ajqt--dispatched-test job tr)))
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
		(insert (format "Completed test run %S\n" id))
		(let ((inhibit-message t))
		  (pp (ajqt-displayable-test-run tr) (current-buffer)))))))
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
    `(,tbl ,tr ,jobs)))

(defun ajqt-displayable-test-run (tr)
  `(ajqt-test-run
    (id ,(ajqt--test-run-id tr))
    (table ,(ajq--table-id (ajqt--test-run-table tr)))
    (test ,(ajqt--test-run-test tr))
    (results ,(ajqt--test-run-results tr))
    (log ,(ajqt--test-run-log tr))
    (freq ,(ajqt--test-run-freq tr))
    (max-time ,(ajqt--test-run-max-time tr))
    (on-dispatch ,(not (not (ajqt--test-run-on-dispatch tr))))
    (on-succeed ,(not (not (ajqt--test-run-on-succeed tr))))
    (on-timeout ,(not (not (ajqt--test-run-on-timeout tr))))
    (on-quit ,(not (not (ajqt--test-run-on-quit tr))))))


;;; test queue

(defvar ajqt-test1 
  (ajqt--make-test 10 20 20))

(defvar ajqt-test2 
  (ajqt--make-test 5 20 30))

(defvar ajqt-tbl
  (ajq-make-job-queue 1 2))

(setq slot1 (ajq--alloc-slot ajqt-tbl))
(setq job1 (ajq--reclaim-slot slot1))

(defvar ajqt-test1-run1
  (ajqt--make-test-run
   ajqt-tbl ajqt-test1
   1 nil 't1))

;; (defvar ajqt-test1-run2
;;   (ajqt-run-test ajqt-test1 1 2 't1r5))

;; (defvar ajqt-test1-run3
;;   (ajqt-run-test ajqt-test1 1 (num-processors) 't1r3))


;;(pp (car ajqt-test1-run2) (get-buffer "*scratch*"))

;;(cancel-timer (ajq--table-timer (car ajqt-test1-run2)))

(provide 'async-job-queue-test)
;;; async-job-queue-test.el ends here

;; Local Variables:
;; read-symbol-shorthands: (("ajq-" . "async-job-queue-")("ajqt-" . "async-job-queue-test-"))
;; End:
