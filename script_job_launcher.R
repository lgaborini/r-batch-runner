# Batch-only script
#
# Job definitions:
#    the script runs a default job (specified in file 'job_template.yaml'), 
#    and all jobs in subdirectory 'jobs/'.
#
# All outputs are saved in output folder path_output = 'batch.out'
# The output folder MUST exist.
#--------------------------------------------

rm(list = ls())

library(here)

# Path to batch-runner folder
path_batch_folder <- here('batch', 'r-batch-runner')

# Launcher configuration --------------------------------------------------
#
# Load global options for the batch script
#

library(yaml)

batch_opts <- yaml::read_yaml(file.path(path_batch_folder, 'batch-opts.yaml'))


# Path configuration ------------------------------------------------------

# Job input path
path_jobs <- file.path(path_batch_folder, batch_opts$paths$path_jobs)

# Job output path: must exist!
path_output <- file.path(path_batch_folder, batch_opts$paths$path_output)
stopifnot(dir.exists(path_output))

# Job loader and preloader path
path_job_loader <- file.path(path_batch_folder, batch_opts$paths$path_job_loader)



# Logfile configuration ---------------------------------------------------

library(futile.logger)

logfile <- file.path(path_output, batch_opts$logging$filename)

# flog.appender(appender.file(logfile), name = 'ROOT')
flog.appender(appender.tee(logfile), name = 'ROOT')
# flog.appender(appender.console(), name = 'ROOT')

# flog.threshold(DEBUG, name = 'ROOT')
flog.threshold(INFO, name = 'ROOT')


# A function which writes to log
write_log <- flog.info

# Create empty logfiles
logfile_last <- file.path(path_output, batch_opts$logging$filename_last)
logfile_jobs_success <- file.path(path_output, batch_opts$logging$filename_succeed)
logfile_jobs_fail <- file.path(path_output, batch_opts$logging$filename_fail)

unlink(logfile)
unlink(logfile_jobs_success)
unlink(logfile_jobs_fail)


# Batch job configuration -------------------------------------------------

source(file.path(path_batch_folder, 'batch-utilities/utilities_batch.R'))
source(file.path(path_batch_folder, 'batch-utilities/IFTTT.R'))

# dir.create(path_output, showWarnings = TRUE)

# Will contain jobfiles with success/fail results
job_results <- list(failed = NULL, succeeded = NULL)

# Job definition
# This is run using local paths!
source(path_job_loader, chdir = TRUE)

if (!exists('job_preloader') || !is.function(job_preloader)) {
   stop('Job preloader not defined.')
}

if (!exists('job_loader') || !is.function(job_loader)) {
   stop('Job loader not defined.')
}

# Wrap the job loader
job_loader_safe <- purrr::safely(job_loader, quiet = FALSE)

# Batch job definition -------------------------------------------------------------------

# Queue definition: batch ends when queue is empty

# Template job file
# It is well-formatted YAML, contains a very fast test case, easy to check.
job_file_start <- file.path(path_batch_folder, 'job_template.yaml')
# job_parameters <- yaml.load_file(job_file_start)

# Load job chain: default, the template
# jobs_in_queue <- list(job_file_start)
jobs_in_queue <- list()

# Load jobs from folder
jobs_in_queue <- c(jobs_in_queue, list.files(path_jobs, pattern = '*.yaml', full.names = TRUE))

n_jobs <- length(jobs_in_queue)
i_job <- 0

if (identical(jobs_in_queue, list(job_file_start))) {
   warning('No jobs found: only processing template job!')
   flog.warn('No jobs found: only processing template job!')
}


# Job preloading
# e.g. to load data, set seed, etc.
#
job_preloader(log_writer = write_log, path_output = path_output)

while (length(jobs_in_queue) > 0) {
   
   # Process the job queue ---------------------------------------------------------
   
   flog.info('Processing a new job.')
   
   # Pop the first job in queue
   job_file <- jobs_in_queue[[1]]
   jobs_in_queue <- jobs_in_queue[-1]
   i_job <- i_job + 1
   
   
   # Load the YAML configuration
   job_parameters <- yaml.load_file(job_file)

   
   # Queue the next job, if present
   if (!is.null(job_parameters$job$next.job)) {
      if (job_parameters$job$next.job == job_file) {
         flog.fatal('Job loop detected!')
         stop('Job loop detected!')
      }
      jobs_in_queue <- c(jobs_in_queue, job_parameters$job$next.job)
   }
   
   flog.info("Running job file '%s' [%d of %d].", job_file, i_job, n_jobs)
   
   # Job run -------------------------------------------------------------------

   job_success <- TRUE
   
   # Time and run
   time_start <- proc.time()['elapsed']
   time_start_global <- Sys.time()
   
   # Call the job loader
   job_output <- withCallingHandlers(
      
      {
         
         results_safe <- job_loader_safe(
            job_parameters = job_parameters,
            log_writer = write_log,
            path_output = path_output
         )
         
         # Re-throw error but re-catch it later
         if (!is.null(results_safe$error)) {
            signalCondition(results_safe$error)
         }
         
         # Return the wrapped output
         if (batch_opts$job_results$save_failures) {
            results_safe
         } else {
            # NULL if error
            results_safe$result
         }
      },
      
      warning = function(w) {
         flog.warn('Job returned a WARNING. Reason:\n%s\n', w)
      },
      
      error = function(e) {
         flog.error('Job failed. Reason:\n%s\n', e)
         
         job_success <<- FALSE
      },
      
      finally = {
         
         # Job timing 
         time_end_global <- Sys.time()
         time_total_sec <- as.numeric(difftime(time_end_global, time_start_global, units = 'secs'))
         flog.info(sprintf("Job file '%s' finished. Total time: %.2f seconds.", job_file, time_total_sec))
         
         # Notify IFTTT for longer jobs
         if (time_total_sec >= batch_opts$notify$min_time) {
            IFTTT_notify(value1 = sprintf('End job "%s".', job_name), value2 = job_file)
         }
         flog.info('---')
      }
   )
   
   if (job_success == TRUE) {
      flog.debug('Job "%s" succeeded.', job_file)
      
      # Append to succeeded jobs
      job_results$succeeded <- c(job_results$succeeded, job_file)
      write(job_results$succeeded, file = logfile_jobs_success, append = TRUE)
      
      # Do something with job_output: save
      if (!is.null(job_output)) {
         flog.debug('Have job output!')
         flog.debug(str(job_output))
         
         job_file_basename <- tools::file_path_sans_ext(basename(job_file))
         file_output <- normalizePath(file.path(path_output, paste0(job_file_basename, '.RData')), mustWork = FALSE)
         
         flog.info('Saving output in file "%s', file_output, '"')
         save(job_output, file = file_output)
      }

   } else {
      # Append to failed jobs
      job_results$failed <- c(job_results$failed, job_file)
      write(job_results$failed, file = logfile_jobs_fail, append = TRUE)
   }
   
}  # end job queue



write_log('---')
write_log("Batch finished.")
IFTTT_notify(value1 = 'End batch.')
write_log('---')

write_log(sprintf("Failed jobs: %d/%d", sum(length(job_results$failed)), n_jobs))
if (is.null(job_results$failed)){ 
   write_log('  [none]')
} else {
   write_log(paste('-', basename(job_results$failed)))
}

write_log(sprintf("Succeeded jobs: %d/%d", sum(length(job_results$succeeded)), n_jobs))
if (is.null(job_results$succeeded)) {
   write_log('  [none]')
} else {
   write_log(paste('-', basename(job_results$succeeded)))
}

# Save logfile to batch_output
invisible(file.copy(from = logfile, to = logfile_last, overwrite = TRUE, copy.date = TRUE))



# Run the optional termination command
if (exists('job_parameters') && !is.null(job_parameters$job$run.on.terminate)) {
   write_log('Running termination commands...')
   system(job_parameters$job$run.on.terminate)   
}

write_log('---')
write_log('Batch finished.')
