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

logfile <- normalizePath(file.path(path_output, batch_opts$logging$filename), mustWork = FALSE)

# Parallel configuration

# flog.appender(appender.file(logfile), name = 'ROOT')
flog.appender(appender.tee(logfile), name = 'ROOT')
# flog.appender(appender.console(), name = 'ROOT')

flog.threshold(DEBUG, name = 'ROOT')
# flog.threshold(INFO, name = 'ROOT')

flog.layout(layout.worker)


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

source(file.path(path_batch_directory, 'batch-utilities/utilities_batch.R'), local = TRUE)
source(file.path(path_batch_directory, 'batch-utilities/IFTTT.R'), local = TRUE)

# dir.create(path_output, showWarnings = TRUE)

# Will contain jobfiles with success/fail results
job_results <- list(failed = NULL, succeeded = NULL)

# Job definition
# This is run using local paths!
source(path_job_loader, chdir = TRUE, local = TRUE)

job_loader <- function(log_writer) {
   log_writer('job_loader: trying computation')
   x <- rbinom(1, 10, 0.5)
   log_writer('job_loader: got %d', x)
   if (x %% 2 == 0) {
      stop('job_loader: fail!')
   } else {
      if (x %% 3 == 0) {
         warning('job_loader: warning!')
      } else {
         log_writer('job_loader: success!')   
      }
   }
   return(x)
}

if (!exists('job_preloader') || !is.function(job_preloader)) {
   stop('Job preloader not defined.')
}

if (!exists('job_loader') || !is.function(job_loader)) {
   stop('Job loader not defined.')
}

# Wrap the job loader
job_loader_safe <- purrr::safely(job_loader, quiet = FALSE)
job_loader_quiet <- purrr::quietly(job_loader)

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



# Job preloading
# e.g. to load data, set seed, etc.
#
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
   
   my_pid <- Sys.getpid()

   # Configure worker log ----------------------------------------------------
   # Overwrite base logger to log to separate file
   
   logfile_local <- logfile %>% 
      fs::path_ext_remove(path = .) %>% 
      paste0(., '_', my_pid, '.log')
   
   flog.appender(appender.tee(logfile_local), name = 'ROOT')
   
   
   # Process the job queue ---------------------------------------------------------
   
   flog.info('Processing a new job.')
   
   # Pop the first job in queue
   job_file <- jobs_in_queue[[i_job]]
   
   # Load the YAML configuration
   job_parameters <- yaml::yaml.load_file(job_file)
   
   
   flog.info("[Job %d of %d - %.0f%%] Running job file '%s'.", i_job, n_jobs, i_job/n_jobs * 100, job_file)
   
   # Setup job output container, if job has output
   
   job_file_basename <- tools::file_path_sans_ext(basename(job_file))
   file_output <- normalizePath(file.path(path_output, paste0(job_file_basename, '.RData')), mustWork = FALSE)
   
   if (file.exists(file_output)) {
      flog.info('Job already exists.')
      if (batch_opts$job_results$overwrite) {
         flog.info('Overwriting!')
      } else {
         flog.info('Skipping.')
         # next
         return(list(job_file = job_file, job_success = TRUE, job_status = 'skip'))
      }
   }
   
   # Job run -------------------------------------------------------------------
   
   job_status <- 'success'
   job_success <- TRUE
   
   # Call the job loader
   # job_output <- withCallingHandlers(
   #    {
   #       
   #       # results_safe <- job_loader_safe(
   #       #    job_parameters = job_parameters,
   #       #    log_writer = write_log,
   #       #    path_output = path_output
   #       # )
   #       results_safe <- job_loader_safe(log_writer = write_log)
   #       
   #       # Re-throw error but re-catch it later
   #       # if (!is.null(results_safe$error)) {
   #       #    signalCondition(results_safe$error)
   #       # }
   # 
   #       # Return the wrapped output
   #       results_safe
   #    },
   #    
   #    warning = function(w) {
   #       flog.warn('Job returned a WARNING. Reason:\n%s\n', w)
   #    },
   #    error = function(e) {
   #       flog.error('Job failed. Reason:\n%s\n', e)
   #       
   #       job_success <<- FALSE
   #    }
   # )
   
   # results_safe <- job_loader_safe(log_writer = write_log)
   #  
   results_safe <- withCallingHandlers(
      withRestarts({
            job_loader(log_writer = write_log)
         },
         muffleWarning = function(w){
            list(result=NULL, error = NULL, warning = w)
            },
         muffleStop = function(e){
            list(result=NULL, error = e, warning = NULL)
         }
      ),
      warning = function(w){
         flog.warn('Job returned a WARNING. Reason:\n%s\n', w)
         job_success <<- TRUE
         job_status <<- 'warn'
         invokeRestart("muffleWarning")

      },
      error = function(e){
         flog.error('Job failed. Reason:\n%s\n', e)
         job_success <<- FALSE
         job_status <<- 'error'
         invokeRestart("muffleStop", e)
         
      }
   )
   
   flog.info(sprintf("[Job %d of %d - %.0f%%] Job file '%s' finished.", i_job, n_jobs, i_job/n_jobs * 100, job_file))
   flog.info('---')
   
   flog.debug('Returned: ')
   flog.debug(str(results_safe))
   
   # results_safe <- job_loader_quiet(log_writer = write_log)
   
   # if (!is.null(results_safe$error)) {
   #    job_output <- list(result=NULL, error=results_safe$error)
   # } else {
   #    
   # }
   
   # if (!is.null(results_safe$error)) {
   #    flog.error('Job failed. Reason:\n%s\n', results_safe$error)
   #    job_output <- NULL
   #    
   # } else {
   #    if (!length(results_safe$warnings) == 0) {
   #       flog.error('Job returned a WARNING. Reason:\n%s\n', results_safe$warnings)
   #       job_success <- TRUE
   #    }
   #    job_output <- results_safe
   # }
   
   flog.debug('Continuing foreach loop...')
   
   # Imitate purrr::safely
   if (job_success == TRUE) {
      job_output <- list(result=results_safe, error=NULL)
   } else {
      job_output <- list(result=NULL, error=results_safe$error)
   }
   
   if (job_success == TRUE) {
      flog.debug('Job "%s" succeeded.', job_file)
      
      # Do something with job_output: save
      if (!is.null(job_output$result)) {
         flog.debug('Have job output!')
         flog.debug(str(job_output$result))
         
         flog.info('Saving output in file "%s"', file_output)
         save(job_output, file = file_output)
      }
      
   } else {
      if (job_success != 'skip') {
         flog.debug('Job "%s" failed.', job_file)
      }
   }
   return(list(job_file = job_file, job_success = job_success, job_status = job_status))
}  # end job queue

write_log('---')
write_log("Batch finished.")

flog.debug('---')
flog.debug('Destroying cluster')
stopImplicitCluster()
flog.debug('Cluster destroyed.')

IFTTT_notify(value1 = 'End batch.')
write_log('---')

# write_log(sprintf("Failed jobs: %d/%d", sum(length(job_results$failed)), n_jobs))
# if (is.null(job_results$failed)){ 
#    write_log('  [none]')
# } else {
#    write_log(paste('-', basename(job_results$failed)))
# }
# 
# write_log(sprintf("Succeeded jobs: %d/%d", sum(length(job_results$succeeded)), n_jobs))
# if (is.null(job_results$succeeded)) {
#    write_log('  [none]')
# } else {
#    write_log(paste('-', basename(job_results$succeeded)))
# }


# Save logfile to batch_output
invisible(file.copy(from = logfile, to = logfile_last, overwrite = TRUE, copy.date = TRUE))

# flog.debug(print(str(list_results)))

tbl_results <- list_results %>% map_dfr(as_tibble)


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
