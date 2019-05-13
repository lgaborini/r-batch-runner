# This converts job functions to batch-runner functions
#
# All paths are relative to the loader!
#

source('sample_job.R')

#' The job loader
#' 
#' The job loader: takes parameters from YAML, converts them to run an external function using those parameters.
#' In this case, it runs a sample job.
#'
#' @param job_parameters job parameters as read from the YAML job file
#' @param log_writer a function which writes to log
#' @param path_output path to output folder
#' @return the original return value
job_loader <- function(job_parameters, log_writer, path_output) {
   
   # Ugly hack to skip "job.parameters$" indexing everywhere
   # invisible(lapply(job_parameters, function(x) list2env(x, envir = .GlobalEnv)))
   
   write_log("---")
   write_log(sprintf("This is %s on %s (%s).", Sys.info()['nodename'], Sys.info()['machine'], Sys.info()['sysname']))
   write_log(sprintf("Running job name '%s'.", job_parameters$job$job_name))
   
   if (is_AWS()) {
      write_log("Running on AWS.")
   } else {
      write_log("NOT running on AWS.")
   }
   write_log("---")
   
	
	output <- run_case(job_parameters, log_writer, path_output)
	output
}



#' The job preloader
#' 
#' The job preloader.
#' This is executed once per batch session, before the parameter sweep.
#' It is useful to load data.
#'
#' @param log_writer a function which writes to log
#' @param path_output path to output folder
#' @return nothing
job_preloader <- function(log_writer, path_output) {
   
   load_data(log_writer = log_writer)
   
   
}
