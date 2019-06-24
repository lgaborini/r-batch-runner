# Batch-only script
#
# Job definitions:
#    the script runs a default job (specified in file 'job_template.yaml'), 
#    and all jobs in subdirectory 'jobs/'.
#
# All outputs are saved in output directory path_output = 'batch.out'
# The output directory MUST exist.
#--------------------------------------------

rm(list = ls())

library(here)
library(yaml)
library(futile.logger)

# Parallel processing
library(foreach)
library(parallel)
library(doParallel)

# Path to batch-runner directory
path_batch_directory <- here('batch', 'r-batch-runner')

# Launcher configuration --------------------------------------------------
#
# Load global options for the batch script
#


batch_opts <- yaml::read_yaml(file.path(path_batch_directory, 'batch-opts.yaml'))


# Path configuration ------------------------------------------------------

# Job input path
path_jobs <- file.path(path_batch_directory, batch_opts$paths$path_jobs)

# Job output path: must exist!
path_output <- file.path(path_batch_directory, batch_opts$paths$path_output)
stopifnot(dir.exists(path_output))

# Job loader and preloader path
path_job_loader <- file.path(path_batch_directory, batch_opts$paths$path_job_loader)
# path_job_loader <- file.path(path_batch_directory, 'job-scripts', 'job_loader.R')



# Logfile configuration ---------------------------------------------------

layout.worker <- function(level, msg, ...){
   the.time <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   if (length(list(...)) > 0) {
      parsed <- lapply(list(...), function(x) ifelse(is.null(x), 
                                                     "NULL", x))
      msg <- do.call(sprintf, c(msg, parsed))
   }
   sprintf("%s [%s][PID %d] %s\n", names(level), the.time, Sys.getpid(), msg)
}

# Set logfile paths
logfile <- normalizePath(file.path(path_output, batch_opts$logging$filename), mustWork = FALSE, winslash = '/')
logfile_last <- file.path(path_output, batch_opts$logging$filename_last)
logfile_jobs_success <- file.path(path_output, batch_opts$logging$filename_succeed)
logfile_jobs_fail <- file.path(path_output, batch_opts$logging$filename_fail)

# Rotate last logfile to batch_output
if (file.exists(logfile)){
   invisible(file.copy(from = logfile, to = logfile_last, overwrite = TRUE, copy.date = TRUE))
}

# Parallel configuration

# flog.appender_fun <- appender.console
flog.appender_fun <- appender.tee
# flog.appender_fun <- appender.file
flog.appender(flog.appender_fun(logfile), name = 'ROOT')

# flog_thr_level <- INFO
flog_thr_level <- DEBUG
flog.threshold(flog_thr_level, name = 'ROOT')

flog.layout(layout.worker)


# A function which writes to log
write_log <- flog.info

# Empty logfiles
unlink(logfile)
unlink(logfile_jobs_success)
unlink(logfile_jobs_fail)


# Batch job configuration -------------------------------------------------

source(file.path(path_batch_directory, 'batch-utilities/utilities_batch.R'), local = TRUE)
source(file.path(path_batch_directory, 'batch-utilities/IFTTT.R'), local = TRUE)

# dir.create(path_output, showWarnings = TRUE)

# Will contain jobfiles with success/fail results
job_results <- list(failed = NULL, succeeded = NULL)



# Job definition
# This is run using local paths!
flog.info('Reading job loader/preloader definitions.')
source(path_job_loader, chdir = TRUE, local = TRUE)

# # sample job which explores conditions
# job_loader <- function(log_writer) {
#    log_writer('job_loader: trying computation')
#    x <- sample.int(9, 1)
#    log_writer('job_loader: got %d', x)
#    is_clean_success <- TRUE
#    
#    if (x %% 2 == 0) {
#       is_clean_success <- FALSE
#       stop('job_loader: fail, got even number!')
#    }
#    if (x %% 3 == 0) {
#       is_clean_success <- FALSE
#       warning('job_loader: warning, got multiple of 3!')
#    }
#    if (x == 7) {
#       is_clean_success <- FALSE
#       warning('job_loader: obtained 7, throwing warning 1/2')
#       warning('job_loader: obtained 7, throwing warning 2/2')
#    }
#    if (x == 9) {
#       is_clean_success <- FALSE
#       warning('job_loader: obtained 9, throwing warning')
#       stop('job_loader: obtained 9, ERROR.')
#    }
#    if (x == 1) {
#       is_clean_success <- FALSE
#       warning('job_loader: obtained 1, throwing warning 1/2')
#       stop('job_loader: obtained 1, ERROR')
#       warning('job_loader: obtained 1, throwing warning 2/2')
#    }
#    log_writer('job_loader: reached end of function, %s', ifelse(is_clean_success, 'successfully', 'with warnings'))
#    return(x)
# }


# Wrap the job loader
job_loader_safe <- purrr::safely(job_loader, quiet = FALSE)
job_loader_quiet <- purrr::quietly(job_loader)


if (!exists('job_preloader') || !is.function(job_preloader)) {
   stop('Job preloader not defined.')
}

if (!exists('job_loader') || !is.function(job_loader)) {
   stop('Job loader not defined.')
}

flog.info('Job defined.')

# Batch job definition -------------------------------------------------------------------

# Queue definition: batch ends when queue is empty

# Template job file
# It is well-formatted YAML, contains a very fast test case, easy to check.
job_file_start <- file.path(path_batch_directory, 'job_template.yaml')
# job_parameters <- yaml.load_file(job_file_start)

# Load job chain: default, the template
# jobs_in_queue <- list(job_file_start)
jobs_in_queue <- list()

# Load jobs from directory
jobs_in_queue <- c(jobs_in_queue, list.files(path_jobs, pattern = '*.yaml', full.names = TRUE))

n_jobs <- length(jobs_in_queue)
i_job <- 0

if (identical(jobs_in_queue, list(job_file_start))) {
   warning('No jobs found: only processing template job!')
   flog.warn('No jobs found: only processing template job!')
}

flog.info('Found %d jobs.', n_jobs)

# Job preloading
# e.g. to load data, set seed, etc.
#
flog.info('Running job preloader.')
job_preloader(log_writer = write_log, path_output = path_output)




# Test run job
# 
# seed_candidates <- seq(100)
# s <- 7
# for (s in seed_candidates) {
#    
#    print(paste('Seed ', s))
# 
#    
#    set.seed(s); r <- job_loader_safe(
#       job_parameters = readRDS('pm_job_parameters.rds'),
#       log_writer = invisible,
#       path_output = list()
#    )
#    
#    if (!is.null(r$error)) {
#       print(s)
#       stop('Found failure case.')
#    }
#    
# }
# 
# stop('Exit')

# Setup parallel processing -----------------------------------------------

if (batch_opts$parallel$run_parallel == FALSE) {
   n_workers_max <- 1
} else {

   if (is.null(batch_opts$parallel$max_workers)) {
      n_workers_max <- max(c(parallel::detectCores() - 1, 1))
   } else {
      n_workers_max <- min(c(batch_opts$parallel$max_workers, parallel::detectCores()))
   }
   
}
# n_workers <- 1
flog.debug('Requested %d parallel workers.', n_workers_max)

if (batch_opts$parallel$run_parallel) {
   registerDoParallel(n_workers_max)
   flog.debug('Parallel framework registered.')
} else {
   registerDoSEQ()
   flog.debug('SEQUENTIAL framework registered.')
}

# from now on we are running inn_workers_max
n_workers <- getDoParWorkers()
flog.debug('Running with %d parallel workers.', n_workers)


list_results <- foreach(i_job = seq_along(jobs_in_queue),
                        .packages = c('futile.logger', 'yaml', 'purrr'),
                        .errorhandling = 'pass',
                        .inorder = FALSE
) %dopar% {
   
   # Used to identify the worker
   my_pid <- Sys.getpid()
   
   # This loop always returns a list, to be processed at the end:
   # list(job_file, job_success, job_status, job_result)
   #
   # - job_file: name of YAML file
   # - job_success: boolean, if TRUE save to disk if job_status != 'warn'
   # - job_status: 'skip', 'error', 'warn'
   # - job_result: the return value of job_loader
   # - job_conditions: list of conditions thrown while running job_loader
   
   foreach_output <- list(
      job_file = NULL,
      job_success = TRUE,
      job_status = 'success',
      job_conditions = list()
   )
   
   # Configure worker log ----------------------------------------------------
   # Overwrite base logger to log to separate file
   
   logfile_local <- logfile %>% 
      fs::path_ext_remove(path = .) %>% 
      paste0(., '_', my_pid, '.log')
   
   flog.appender(flog.appender_fun(logfile_local), name = 'ROOT')
   flog.threshold(flog_thr_level, name = 'ROOT')
   
   
   # Process the job queue ---------------------------------------------------------
   
   flog.info('Processing a new job.')
   
   # Pop the first job in queue
   job_file <- jobs_in_queue[[i_job]]
   foreach_output$job_file <- job_file
   
   # Load the YAML configuration
   job_parameters <- yaml::yaml.load_file(job_file)
   
   
   flog.info("[Job %d of %d - %.0f%%] Running job file '%s'.", i_job, n_jobs, i_job/n_jobs * 100, job_file)
   
   # Setup job output container, if job has output
   
   job_file_basename <- tools::file_path_sans_ext(basename(job_file))
   file_output <- normalizePath(file.path(path_output, paste0(job_file_basename, '.RData')), mustWork = FALSE, winslash = '/')
   
   if (file.exists(file_output)) {
      flog.info('Job already exists.')
      if (batch_opts$job_results$overwrite) {
         flog.info('Overwriting!')
      } else {
         
         flog.info('Skipping.')
         
         foreach_output$job_success <- TRUE
         foreach_output$job_status <- 'skip'
         foreach_output$job_conditions <- list()
         
         # next
         return(foreach_output)
      }
   }
   
   # Job run -------------------------------------------------------------------
   
   job_status_detail <- NULL     # detail for last condition thrown
   
   # Call the job loader
   
   results_safe <- withCallingHandlers(
      withRestarts({
            job_loader(
               job_parameters = job_parameters,
               log_writer = write_log,
               path_output = path_output
            )
         },
         muffleWarning = function(w){},
         muffleStop = function(e){}
      ),
      warning = function(w){
         flog.warn('Job returned a WARNING. Reason:\n%s\n', w)
         foreach_output$job_success <<- foreach_output$job_success && TRUE
         foreach_output$job_status <<- 'warn'
         foreach_output$job_conditions <<- append(foreach_output$job_conditions, list(w))
         job_status_detail <<- w

         invokeRestart("muffleWarning")

      },
      error = function(e){
         flog.error('Job failed. Reason:\n%s\n', e)
         foreach_output$job_success <<- FALSE
         foreach_output$job_status <<- 'error'
         foreach_output$job_conditions <<- append(foreach_output$job_conditions, list(e))
         job_status_detail <<- e
         invokeRestart("muffleStop")
         
      }
   )
   job_status_detail['job_result'] <- results_safe
   
   flog.info(sprintf("[Job %d of %d - %.0f%%] Job file '%s' finished.", i_job, n_jobs, i_job/n_jobs * 100, job_file))
   flog.info(sprintf("[Job %d of %d - %.0f%%] Post processing results.", i_job, n_jobs, i_job/n_jobs * 100))
   flog.info('---')
   
   flog.debug('Returned: ')
   flog.debug(str_str(results_safe))
   flog.debug('Detail of last condition: %s', job_status_detail)
   flog.debug('All conditions: ', str_str(foreach_output$job_conditions))
   flog.debug('Foreach return value:')
   flog.debug(str_str(foreach_output))
   
   flog.debug('Continuing foreach loop...')
   
   # Imitate purrr::safely
   #
   # save list "job_output" structure in RData:
   # - result: NULL if error, else return value of job_loader
   # - error: NULL if no error, else condition
   # - warning: NULL if no warning, else condition
   # - job_conditions: all conditions thrown while running
   # - status: 'success', 'warn', 'error', 'skip'
   
   job_output <- list(
      result = NULL,
      error = NULL,
      warning = NULL,
      job_conditions = foreach_output$job_conditions,
      status = foreach_output$job_status
   )
   
   if (foreach_output$job_success == TRUE) {
      job_output['result'] <- results_safe
      job_output['error'] <- list(NULL)
   } else {
      job_output['result'] <- list(NULL)
      job_output['error'] <- results_safe
   }
   
   if (foreach_output$job_success == TRUE) {
      flog.debug('Job "%s" succeeded.', job_file)
      
      # Do something with job_output: save
      if (!is.null(job_output$result)) {
         flog.debug('Have job output!')
         flog.debug(str_str(job_output))
         
         flog.info('Saving output in file "%s"', file_output)
         save(job_output, file = file_output)
      }
      
   } else {
      if (foreach_output$job_success != 'skip') {
         flog.debug('Job "%s" failed.', job_file)
      }
   }
   return(foreach_output)
}  # end job queue

write_log('---')
write_log("Batch finished.")

flog.debug('---')
flog.debug('Destroying cluster')
stopImplicitCluster()
flog.debug('Cluster destroyed.')

IFTTT_notify(value1 = 'End batch.')
write_log('---')

# tbl_results <- list_results %>% map_dfr(as_tibble)
tbl_results <- list_results %>% 
   map(enframe) %>% 
   map_dfr(spread, 'name', 'value') %>% 
   mutate_at(vars(job_file, job_status, job_success), unlist)

write_log('Succeeded jobs: ')
tbl_results %>% 
   filter(job_status %in% c('success', 'warn')) %>% 
   glue::glue_data('- {basename(job_file)} ({job_status})') %>% 
   stringr::str_flatten('\n') %>% 
   write_log()

write_log('FAILED jobs: ')
tbl_results %>% 
   filter(job_status == 'error') %>% 
   glue::glue_data('- {basename(job_file)}') %>% 
   stringr::str_flatten('\n') %>% 
   write_log()

write_log('Skipped jobs: ')
tbl_results %>% 
   filter(job_status == 'skip') %>% 
   glue::glue_data('- {basename(job_file)}') %>% 
   stringr::str_flatten('\n') %>% 
   write_log()

# Append to failed jobs 
tbl_results %>% 
   filter(job_status == 'error') %>% 
   glue::glue_data('{basename(job_file)} ({job_status})') %>% 
   write(file = logfile_jobs_fail, append = TRUE)
   
# Append to succeeded jobs 
tbl_results %>% 
   filter(job_status %in% c('success', 'warn')) %>% 
   glue::glue_data('{basename(job_file)} ({job_status})') %>% 
   write(file = logfile_jobs_success, append = TRUE)
   
write_log('---')
write_log('Batch finished.')
