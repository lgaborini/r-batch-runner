# Sample job
#
# Does not depend on the structure of batch-runner.
#
source('requirements/job_requirements.R')


# This prints the parameter values inside each sweep
run_case <- function (job_parameters = NULL, log_writer = print, path_output) {

   log_writer('Running job!')

   log_writer('parameters: ')
   log_writer(job_parameters)
   
   if (job_parameters$params$param_1 == 'a') {
      stop('Failure! param_1 = "a"')
   }
   
   log_writer('Job finished!')
   
   invisible(NULL)
}

# This reads some data
load_data <- function(log_writer = print) {
   
   log_writer('Preloading stage!')
   log_writer('Loading data:')
   log_writer(head(readRDS(file.path('data', 'data.rds')), 5))
   
}
