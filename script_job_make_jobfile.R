# Create a job.yaml from a template
# 
# Can introduce parameter sweeps.
#
# Each job has name "{basename}_{parameters}_{uuid}".
# Each job file is named "jobs/{basename}_{parameters}_{uuid}.yaml"
# Each UUID is truncated to 12 digits.

library(here)

path_batch_directory <- here('batch', 'r-batch-runner')

source(file.path(path_batch_directory, 'batch-utilities', 'utilities_batch.R'))

library(yaml)
library(uuid)
library(glue)
library(dplyr)
library(purrr)


# Launcher configuration --------------------------------------------------

batch_opts <- yaml::read_yaml(file.path(path_batch_directory, 'batch-opts.yaml'))

# Launcher options

# Setup output directory for yaml files
# Create if empty
path_jobs <- file.path(path_batch_directory, batch_opts$paths$path_jobs)

# Clear old jobs
if (batch_opts$job_creation$clear_old_jobs) {
   unlink(path_jobs, recursive = TRUE)
}
   
dir.create(path_jobs, showWarnings = FALSE)


# Parameter sweep ---------------------------------------------------------

# Define jobfile basename
jobfile_basename_default <- 'FourierLR'

# Batch job purpose: description, comments, ...
jobfile_batch_description <- ''

# Define parameter sweeps
#    some parameters can be fixed: set them as list singletons, or outside lists

# Generate parameter sweep
df_combinations <- purrr::cross_df(list(
   
      # Data selection
      # - must be a list of lists!
      # - singletons are p.ex., list(list('1'))
      which_harmonics = list(
         # list('1'),    # only one variable! 
         list('1', '2', '3')
         # list('2'),
         # list('3'), 
         # list('4')
      ),

      # which character to consider for ref/quest/background
      which_character = c('all'),
      # words, letters or everything
      which_region = c('all'),
      
      ## Comparison configuration
      ## - random: randomly choose ref/quest
      ## - paired: try all combinations
      
      # writer_comparison = 'paired',
      writer_comparison = 'random',
      
      ## For writer_comparison = 'random': which Hd to sample from
      ## - 'same': Hd = Hp
      ## - 'unrelated: Hd = Hd_u
      ## - 'twin: Hd = Hd_t
      Hd_source = list('same', 'unrelated', 'twin'),
      
      ## For writer_comparison = 'paired': ignored
      ## Hd is set to be all possible writer_quest
      
      # Hd_source = list('any'),
      
      ## Sample selection
      k_ref = list(5, 10, 20, 50) %>% map(as.integer),
      k_quest = list(1, 2, 5, 10, 20, 50) %>% map(as.integer),
      
      
      # Iteration options
      n_iter = as.integer(100000),
      burn_in = as.integer(10000),
      
      # How many times a particular combination of parameters is repeated
      n_trials = seq(10),
      
      # Prior and initialization
      use_priors = 'ML',
      use_init = 'random',
      
      # Contents of background data
      split_background = 'outside'
      
   ))


# Refine combinations -----------------------------------------------------

combination_fields <- colnames(df_combinations)

# Refine here...

# Balanced sample: reference = questioned
# df_combinations <- df_combinations %>%
#    filter(k_ref == k_quest)

# More reference than questioned samples
df_combinations <- df_combinations %>% 
   filter(k_ref >= k_quest)

# Balanced large samples, or reference-prevalent unbalanced small samples 
df_combinations <- df_combinations %>% 
   mutate(
      is_balanced = k_ref == k_quest,
      is_large = k_ref > 10 && k_quest > 10
   ) %>% 
   filter(k_ref >= k_quest) %>% 
   filter(is_large && is_balanced || !is_large)

cat('Generated configurations:\n')
print(df_combinations)

# Remove fields
df_combinations <- df_combinations %>% 
   select(one_of(combination_fields))

# Generate job names ------------------------------------------------------

# Define job name pattern
#
# - `basename` will be substituted by a fixed string 
# - `uuid` will be randomly generated
# - column names from df_combinations can be used
#
str_filename_pattern <- '{basename}_t={n_trials}_h={which_harmonics}_char={which_character}_comp={writer_comparison}_Hd={Hd_source}_{uuid}'

# Function which generates a file name from parameter combinations
#
# Also adds further fields, as required by the format string
make_file_name <- function(df_combinations) {
   
   df_combinations_extended <- df_combinations %>% 
      mutate(
         basename = jobfile_basename_default,
         uuid = make_uuid(12)
      )
   
   # Collapse lists in filenames
   df_combinations_friendly <- df_combinations_extended %>% 
      mutate_if(is.list, ~ purrr::map_chr(.x, paste, collapse = ''))
   
   
   filenames <- glue_data(df_combinations_friendly, str_filename_pattern)
   filenames
}

cat('Expected file names:\n')

df_combinations %>% 
   make_file_name() %>% 
   head(10) %>% 
   print()
# 
# if (interactive()) {
#    if (rstudioapi::isAvailable())
#       View(df_combinations)
# }


r <- readline(prompt = "Continue with job creation? [yn] (default: \"y\") ")
if (!(identical(r, "y") || identical(r, ""))) stop('Exiting without saving.')

message('Starting job creation.')

# Generate job files ------------------------------------------------------

n.combinations <- nrow(df_combinations)

# Create progress bar
pb <- dplyr::progress_estimated(n.combinations)

if (batch_opts$job_creation$verbose_output){
   # Log to screen
   cat_cmd <- cat
} else {
   # Log to file, create progress bar
   cat_cmd <- function(...) invisible(NULL)
}


r <- 1
for (r in seq(n.combinations)) {

   cat_cmd(sprintf('\n* Making jobfile %d of %d.\n', r, n.combinations))
   
   # Load the template, then overwrite it
   yaml_template <- yaml::yaml.load_file(file.path(path_batch_directory, 'job_template.yaml'))
   yaml_params <- yaml_template
   

   # Modify YAML parameters -------------------------------------------------------
   
   # Set current parameter sweep as "params" section in YAML
   
   # yaml_params$params$param_1 <- df_combinations[r, 'param_1']
   yaml_params$params <- df_combinations[r, ]
   
   # Print to screen
   # yaml_params$params %>% glimpse() %>% print()
   
   
   # Set batch job description
   yaml_params$job$batch_description <- jobfile_batch_description

   # Generate job filename ---------------------------------------------------
   
   # Generate a job filename description using UUID (without "-")
   # Create the output filename from parameters and the string template
   jobfile_name <- make_file_name(df_combinations[r, ])
   
   # Set the name in YAML job file
   yaml_params$job$job_name <- jobfile_name
   
   # Full path for YAML job file
   jobfile_name_full <- normalizePath(file.path(path_jobs, paste0(jobfile_name, '.yaml')), mustWork = FALSE)
   

   # Write to YAML -----------------------------------------------------------

   cat_cmd(sprintf('Making job "%s"\n', jobfile_name))

   if (file.exists(jobfile_name_full)) {
      stop(paste('File', jobfile_name_full, ' already exists!'))
   }
   write(as.yaml(yaml_params), jobfile_name_full)
   
   cat_cmd(sprintf('Wrote jobfile "%s".\n', jobfile_name_full))
   
   # Tick the progress bar if not verbose
   if (!batch_opts$job_creation$verbose_output){
      print(pb$tick())
   }
}
