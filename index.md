# Short user documentation

### Submit a DAG

To submit a DAG to Slurm use the `slurm_submit_dag` command:

```
slurm_submit_dag <dag-file>
```

Check the options of the command with `slurm_submit_dag --help`.

This will run a *slurm_dagman* process that will parse the DAG file and submit
and monitor the Slurm jobs. The *slurm_dagman* process will log its activity
into an out file which by default has the same name of the dag file plus the
extension ".slurm_dagman.out".

### DAG file syntax

The syntax of the DAG file is similar to the one used in [HTCondor DAGMan](https://htcondor.readthedocs.io/en/latest/users-manual/dagman-workflows.html).
The following four constructs are recognized by SlurmDagman in a DAG file:

```
JOB dag-node slurm-submission-file
VARS dag-node var-name=var-value [...]
RETRY dag-node max-retries
PARENT parent-dag-node[,...] CHILD child-dag-node[,...]
```

### Configuration of an individual slurm_dagman process

Each *slurm_dagman* process writes a process configuration file in the same
directory and with the same name as the DAG file with an additional extension
".slurm_dagman.cfg". The process configuration file is the way of communicating
with the *slurm_dagman* process. This file is read by the *slurm_dagman* process
at the end of each iteration, updating its internal working parameters.

The following parameters are configurable via the *slurm_dagman* process
configuration file. Each parameter has a hard-coded default value in the
SlurmDagman package. The default values can be overridden in the package
configuration file (except for `drain` and `cancel`, where only `False`
makes sense as a default value). They can also be passed specific
values for each *slurm_dagman* process as options in the `slurm_submit_dag`
command. 

- `sleep_time`: (int) Specifies the time -in seconds- that *slurm_dagman* will
sleep between two iteartions. A non-positive value means do not sleep. The
hard-coded default is 120.

- `max_jobs_queued`: (int) Specifies the maximum number of jobs that *slurm_dagman*
can put in the Slurm queue. A non-positive value means no maximum limit. The
hard-coded default is 500.

- `max_jobs_submit`: (int) Specifies the maximum number of jobs that *slurm_dagman*
is allowed to submit in each iteration. A non-positive value means no maximum
limit. The hard-coded default is 0.

- `submit_wait_time`: (int) Specifies how much time -in seconds- should *slurm_dagman*
wait (sleep) between two consecutive job submissions. A non-positive value
means do not wait. The hard-coded default is 2.

- `drain`: (boolean) When set to True, *slurm_dagman* will stop submitting new
jobs; it will keep monitoring the status of currently queued jobs until there
are no more jobs in the queue, at which point it will write a rescue DAG file
(unless the DAG has completed) and exit.

- `cancel`: (boolean) When set to True, *slurm_dagman* will cancel the jobs
that are currently queued, write a rescue DAG file and exit.

### Cancel a running DAG

A running *slurm_dagman* process can be cancelled by setting `cancel = True`
in the process configuration file or by running the `slurm_cancel_dag` command.
The `slurm_cancel_dag` command will only work if there is only one *slurm_dagman*
process running owned by the current user. The `slurm_cancel_dag` sends a SIGTERM
signal to the *slurm_dagman* process by means of a `kill -9 <slurm_dagman-pid>`.
SIGTERM signals are trapped by *slurm_dagman*, which then terminates the DAG
(cancels queued jobs) and writes a rescue DAG file. Equivalently, a user can
run a `kill -9 <slurm_dagman-pid>` by hand if the PID of the *slurm_dagman* process
is known (each time a *slurm_dagman* process starts, it will log its PID into
the .slurm_dagman.out file).

### Translate a HTCondor DAG file and submission files to Slurm

Using the `condor_dag_to_slurm_dag` command one can translate a DAG file that
is intended to be used with HTCondor DAGMan together with a set of HTCondor
submission files into a corresponding set of Slurm submission files and a dag
file to be used by SlurmDagman:

```
condor_dag_to_slurm_dag <condor-dag-file>
```

Check the options of the command with `condor_dag_to_slurm_dag --help`.
For example, one can specify that the job payload should be run with
singularity in the Slurm job.

Note: Only some basic variables available in a HTCondor submission file
are translated. The rest is ignored and not translated. Thus, the user
should better check if the translation is correct.
