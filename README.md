# batch-runner

<!-- badges: start -->
<!-- badges: end -->

This folder a framework to run parametrized scripts in batch mode.

- parameters for each job are read from .yaml files in folder `jobs/` (job files)
- job files are generated using the script `script_job_make_jobfile.R`. The included script creates job files by sweeping parameters.
- jobs scripts are specified in folder `job-scripts/`
- a job loader is provided into folder `job-scripts/job_loader`: it is responsible for launching a job, translating the parameters in the job file, and returning the output
- a job preloader can be run, once per batch run, before the parameter sweep
- output is stored in `batch-out/`
- log is stored in `batch-out/workers.log`

Jobs are launched with script `script_job_launcher.R`.

## Components

### Configuration

The batch runner is configurable with the YAML file `batch-opts.yml`.

### Job preloader

The job preloader is a function which accepts these arguments:

- `log_writer`: a function which writes to the logfile
- `path_output`: path to the `batch-out/` folder

It is called once, before the parameter sweep is carried out.

### Job loader

The job loader is a function which accepts these arguments:

- `job_parameters`: everything which is read from the YAML file
- `log_writer`: a function which writes to the logfile
- `path_output`: path to the `batch-out/` folder

It is responsible for calling the job scripts in the `job-scripts/` folder.
Their return value is returned to the main batch loop.

If the return value is not `NULL`, results are saved to disk in the output folder, in a `.RData` file with the same name as the job.

### Logging

Logging is provided by package `futile.logger`.
The default logger is passed to the job loader, which is itself responsible of logging job output.

### Notifications

The framework supports notifciations through IFTTT.    
The IFTTT key is needed: it is supposed to be stored in the environment variable `IFTTT_key`.

If this variable is empty, no notifications are performed.




