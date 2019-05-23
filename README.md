# R-batch-runner

<!-- badges: start -->
<!-- badges: end -->

This directory contains a framework to run parametrized scripts in batch mode.

- parameters for each job are read from `.yaml` files (**job files**) in directory `jobs/` 
- job files are generated using the script `script_job_make_jobfile.R`.    
  The included script creates job files by sweeping parameters.    
  A job file contains an instance of these parameters.
- sample job scripts are specified in directory `job-scripts/`.
- a **job loader** is provided into directory `job-scripts/job_loader.R`: it is responsible for launching a job, translating the parameters in the job file, and returning the output
- a job preloader can be run once per batch run, before the parameter sweep
- output is stored in `batch-out/`
- log is stored in `batch-out/workers.log`
- all these options are configurable: `batch-opts.yaml`

Jobs are launched with script `script_job_launcher.R`.

## Example

A sample job script is included.   

The [sample job](job-scripts/sample_job.R) is a function which receives a list of parameters (`param_1`,
`param_2`, `param_fix`), prints them to the console and sometimes fails, sometimes succeeds (returning them).

The parameters are generated using the script [script_job_make_jobfile.R](script_job_make_jobfile.R).
The function creates a set of jobs by varying the parameters across their cartesian product.

The job creation function creates a set of job files in the directory `/jobs`.
Job creation can be customized by editing the script.

Currently, to each job a job name is associated. The job name is ought to be unique, and is used as the file name for both the job file, and the job output.

Once job files are created, the batch job can be ran by running the script [script_job_launcher.R](script_job_launcher.R).   
This sources the sample job functions, reads all `yaml` files in `/jobs`, executes the function using the provided parameters and saves each return value in directory `/batch-out`.    
Logs are saved in the same directory.

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

The job loader is wrapped into a `purrr::safely` wrapper.
It always returns a list with two components, `error` and `result`. One is always `NULL`.

Errors are signaled and saved in component `error`, return values from the job loader are stored into component `result`.

The return value is always saved to disk in the output directory, in a `.RData` file with the same name as the job.

### Logging

Logging is provided by package `futile.logger`.   
The default logger is passed to the job loader, which is itself responsible of logging job output.

### Notifications

The framework supports notifications through IFTTT.    
The IFTTT key is needed: it is supposed to be stored in the environment variable `IFTTT_key`.

If this variable is empty, no notifications are performed.

Notifications can be disabled in the `batch-opts.yaml` file.

## TODO

- [ ] Separate YAML configuration from manual script editing
- [ ] Define job sweep in YAML file
