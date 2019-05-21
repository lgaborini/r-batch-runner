# R-batch-runner

<!-- badges: start -->
<!-- badges: end -->

This directory contains a framework to run parametrized scripts in batch mode.

- parameters for each job are read from `.yaml` files in directory `jobs/` 
- job files are generated using the script `script_job_make_jobfile.R`.    
  The included script creates job files by sweeping parameters.
- sample job scripts are specified in directory `job-scripts/`.
- a **job loader** is provided into directory `job-scripts/job_loader.R`: it is responsible for launching a job, translating the parameters in the job file, and returning the output
- a job preloader can be run once per batch run, before the parameter sweep
- output is stored in `batch-out/`
- log is stored in `batch-out/workers.log`
- all these options are configurable: `batch-opts.yaml`

Jobs are launched with script `script_job_launcher.R`.

## Components

### Configuration

The batch runner is configurable with the YAML file `batch-opts.yaml`.

All paths are relative to the directory `r-batch-runner`.   

Job scripts can be stored elsewhere. 
The job loader script (`job_loader.R`) is sourced from its directory, and is responsible for setting all other directories.

### Job creation

Jobs are created with the script `script_job_make_jobfile.R`.    

Basically, it reads a template job file (`job_template.yaml`), substitutes parameters and saves as a new `.yaml` file with a parametrized name.

### Job preloader

The job preloader is a function which accepts these arguments:

- `log_writer`: a function which writes to the logfile
- `path_output`: path to the output directory

It is called once, before the parameter sweep is carried out.

### Job loader

The job loader is a function which accepts these arguments:

- `job_parameters`: everything which is read from the YAML file
- `log_writer`: a function which writes to the logfile
- `path_output`: path to the output directory

It is responsible for calling the job scripts in the `job-scripts/` directory.
Their return value is returned to the main batch loop.

If the return value is not `NULL`, results are saved to disk in the output directory in a `.RData` file with the same name as the job.


### Logging

Logging is provided by package `futile.logger`.   
The default logger is passed to the job loader, which is itself responsible of logging job output.

### Notifications

The framework supports notifications through IFTTT.    
The IFTTT key is needed: it is supposed to be stored in the environment variable `IFTTT_key`.

If this variable is empty, no notifications are performed.

Logging can be disabled in the `batch-opts.yaml` file.

## TODO

- [ ] Separate YAML configuration from manual script editing
- [ ] Define job sweep in YAML file
