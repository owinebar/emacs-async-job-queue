# emacs-async-job-queue
Elisp package for managing an arbitrary number of queued async jobs allocated to a limited number of active job slots.

    (async-job-queue-make-job-queue <frequency> [ <number slots> <on-empty> <inactive> <id> ])

Creates a job queue that runs up to *number slots* jobs simultaneously.
The queue polls for results every *frequency* seconds while jobs are running.

When an active job queue has no running or queued jobs, it calls the on-empty continuation with itself as the argument.  If the queue remains empty after that, it will remove the timer used to launch jobs and process results.

A job queue can be active or inactive.  An inactive queue will not launch any jobs from the queue.  An active queue will launch jobs from the queue until all job slots are in use.  Job queues are active by default.  An inactive job queue may be useful in implementing a checkpointed pipeline, where an active job queue has an on-empty continuation that activates a job queue used to accumulate jobs created from the results of jobs completed by the active queue.  

The *id* is only used to create manageable displays of the queue object due to circularity

    (async-job-queue-schedule-job <job-queue> <expression> [ <id> <on-dispatch> <on-finish> <max-time> <on-timeout> <on-quit> ])

Adds a job to *job queue* that will evaluate *expression* in a separate emacs process.

The *id* argument is used for creating manageable displays of the job object.

The *on-dispatch*, *on-finish*, *on-timeout*, and *on-quit* arguments are continuations
called on each respective event.

The *max-time* argument is the number of seconds (wall-clock) the job is allowed to run before terminating the job and calling *on-timeout*.

    (async-job-queue-cancel-job <job>)

Cancels *job*.  Kills the process if already launched or simply removes it from the queue otherwise.  May be used for "quit" action.

    (async-job-queue-cancel-queue <job-queue>)

Cancels all jobs in *job-queue*.  May be useful for "quit" action.

    (async-job-queue-activate-queue <job-queue>)
    (async-job-queue-deactivate-queue <job-queue>)

Activate or deactivate *job-queue*.  

aync-job-queue-test.el has code for testing async-job-queue.  The test jobs are simple expressions to wait random times before returning a value.  The activity of the job queue is printed in a dedicated log buffer.  The test code may be treated as an example of how to use job queues.

