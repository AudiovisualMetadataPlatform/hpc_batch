# Batch Carbonate Jobs

## Overview
The underlying problem with submitting jobs with IU's HPC
Environment is the latency of starting the individual batch jobs.
It is not uncommon for a singular job to wait 12-24 hours, so
when processing large batch of objects the amount of time waiting
far outstrips the amount of time actually using the GPUs

It is possible that submitting several jobs in a row _may_ reduce 
the latency between those jobs, but that's not guaranteed:  if
another user submits a job between our job A & B the latency may 
still be present.  The larger the gap is between job submissions,
the greater the opportunity for other uses to submit something and
introduce latency.

## Possible Solution
The trick to getting the lowest latency here is to smooth out the
time between job submissions on our end and the corresponding job
invocations on the HPC end.

Job submissions from AMP fall into two rate categories:
* One-off jobs:  these are individual runs of a workflow.  The 
span between requests may vary greatly:  from minutes to days.

* Bulk jobs:  these are workflows that are run for many objects as 
part of testing or running an entire collection through AMP.  The
time span between requests would likely be seconds.

The current implementation is a 1:1 relationship between a job 
submission and an HPC invocation.  However, if the implementation 
is changed so it is n:1, the wait for the GPU would be amortized
over several requests, even if they were unrelated.

Further, having the system group all GPU-required tools go through
a single wait-for-GPU tool, the jobs are further amortized which
reduces the overall latency even more.

If multiple machines are submitting HPC jobs, a single instance can
be used to service all of them, reducing the latency even further.

## Implementation Details

There are several systems involved:
* The AMP server, where the galaxy tools run
* The HPC head node, where the HPC data is stored and queueing is 
performed
* The HPC worker, the machine where the GPU-dependent code is 
actually run

There are several programs which work in concert:
* The mgms are responsible for generating a job description and 
waiting for the hpc job to complete.  The description contains the
hpc program to run, options, input files, and output files.
* hpc_submitter runs on the AMP server via cron.  This performs 
several tasks:
    * It looks for outstanding job descriptions.  The information 
    is used to populate work directories on the HPC head node 
    * The hpc_runner will be submitted into the HPC queue on the 
    head node if it is not currently in the queue and it is not 
    running on the HPC worker.
    * If any HPC jobs have completed, the results are gathered and
    made available to the mgms so they may complete
* hpc_runner runs on an HPC worker and is started if it has been 
submitted and resources are available.
    * Any job descriptions found on the HPC head node will be 
    processed until all descriptions have been handled
    * Any descriptions which appear after the hpc_runner has 
    started will be handled until no unhandled descriptions
    remain.
    * Processing the description means running the correct
    singularity container with the arguments and then marking
    the job as finished.
* there is a singularity container for each GPU-required task that 
resides on the HPC head node and will run on the HPC worker when 
called by the hpc_runner


## Directory Structures & File Formats

### Script templates
These template files are used to create a command line which will
invoke a job on the HPC system.  These templates use identifiers
wrapped in braces to indicate replaceable text (i.e. {name})

The description file will provide the values for the arguments as 
well as the input/output files, which will be filled in at 
invocation time.   It is important to note that the replacement
values in the template for files only include the base name -- the
path information is dropped.  Argument values are passed as-is.

Additionally, these variables are available:

|Name|Value|
|----|-----|
|workspace|The full path to the HPC workspace directory|
|scripts|Path to scripts/containers/etc|


A sample template for a singularity container that takes a level 
parameter, an input, and two different outputs:

````
singularity run --nv --bind {workspace}:/mnt {scripts}/my_container.sif --level={level} /mnt/{input} /mnt/{txt_output} /mnt/{json_output}
````



### Job Description File
The file is a YAML-formatted file with these fields:
````
script: <name of the script to run on HPC worker>
args:
    ... key/value pairs for any script arguments ...
input_map:
    <input-filename-key>: <absolute local path of input file>
    ... as needed ...
output_map:
    <output-filename-key>: <absolute local path of output destination>
    ... as needed ..
````

So a job description ("thing.job") to call the template above may read:

````
script: my_container
args:
    level: 6
input_map:
    input: /tmp/foo.mp3
output_map:
    json_output: /tmp/foo.json
    txt_output:  /tmp/foo.txt    
````

Which would call the template as (with a workspace of 
"/scratch/1234"):

````
singularity run --nv --bind /scratch/1234:/mnt my_container.sif --level=6 /mnt/foo.mp3 /mnt/foo.txt /mnt/foo.json
````

and upon completion would create a result file in 
thing.job.finished and place the two output files in
/tmp/foo.txt and /tmp/foo.json.


### Result file
The result file is created on the caller's system to signal the 
caller that the job is complete.  If the result indicates success, 
then the files listed in the outputs section of the description are 
present and complete.

On failure, either because the HPC job failed, the files couldn't
be transferred to/from the HPC system, or any other reason, the 
output files will not be present and failure information will be
included the result file.

The result file format is the job description format, plus this top
level node:
````
job:
    status: <either 'ok', or 'error'>
    message: <"human" readable status message>
    stderr: <contents of remote job stderr>
    stdout: <contents of remote job stdout>
    rc: <remote job return code>
````

On a successful run above the result file may look like:
````
script: my_container
args:
    level: 6
input_map:
    input: /tmp/foo.mp3
output_map:
    json_output: /tmp/foo.json
    txt_output:  /tmp/foo.txt   
job:
    status: ok
    message: processed OK
    stderr:  <whatever was in stderr>
    stdout:  <whatever was in stdout>
    rc: 0
````

but a failure might look like:

````
script: my_container
args:
    level: 6
input_map:
    input: /tmp/foo.mp3
output_map:
    json_output: /tmp/foo.json
    txt_output:  /tmp/foo.txt   
job:
    status: error
    message: Remote command failed with return code 1
    stderr: ERROR: Input file is the wrong format
    stderr: 
    rc: 1
````


### Dropbox on the AMP server
This dropbox holds the submitted job descriptions.  The description 
submitter must ensure a unique filename that ends with ".job"

This is a flat directory with no internal structure.  

### Directory on HPC Head Node / HPC Worker Node

````
hpc_batch/
    scripts/
        # hpc_runner, support scripts, singularity
        # containers, etc.
    templates/
        # script templates
    jobs/
        # dropbox for job descriptions
    workspace/
        # a new subdir created for each running job?
````
