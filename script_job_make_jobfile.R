# Create a job.yaml from a template
# 
# Can introduce parameter sweeps.
#
# Each job has name "{basename}_{parameters}_{uuid}".
# Each job file is named "jobs/{basename}_{parameters}_{uuid}.yaml"
# Each UUID is truncated to 12 digits.

library(here)

path_batch_folder <- here('.')

source(file.path(path_batch_folder, 'batch-utilities', 'utilities_batch.R'))

library(yaml)
library(uuid)
library(glue)
library(dplyr)
library(purrr)


# Launcher configuration --------------------------------------------------

batch_opts <- yaml::read_yaml(file.path(path_batch_folder, 'batch-opts.yaml'))



# Launcher options --------------------------------------------------------

# Setup output directory for yaml files
# Create if empty
path_jobs <- file.path(path_batch_folder, batch_opts$paths$path_jobs)

# Clear old jobs
if (batch_opts$job_creation$clear_old_jobs) {
   unlink(path_jobs, recursive = TRUE)
}
   
dir.create(path_jobs, showWarnings = FALSE)


# Parameter sweep ---------------------------------------------------------

# Define jobfile basename
jobfile_basename_default <- 'jobname'

# Define parameter sweeps
# some parameters can be fixed
list_param_1 <- c('a', 'b')
list_param_2 <- c(0, 1, 2)
list_param_fix <- 1000

# Generate parameter sweep
df_combinations <- purrr::cross_df(list(
      param_1 = list_param_1, 
      param_2 = list_param_2,
      param_fix = list_param_fix
   ))


# Refine combinations -----------------------------------------------------

combination_fields <- colnames(df_combinations)

cat('Generated configurations:\n')
print(df_combinations)

# Refine here...

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
str_filename_pattern <- '{basename}_param1={param_1}_param2={param_2}_{uuid}'

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
      mutate_if(is.list, ~ purrr::map_chr(.x, paste, collapse = ','))
   
   
   filenames <- glue_data(df_combinations_friendly, str_filename_pattern)
   filenames
}

cat('Expected file names:\n')

df_combinations %>% 
   make_file_name() %>% 
   print


# Generate job files ------------------------------------------------------

n.combinations <- nrow(df_combinations)
r <- 1
for (r in seq(n.combinations)) {

   cat(sprintf('Making jobfile %d of %d.\n', r, n.combinations))
   
   # Load the template, then overwrite it
   yaml_template <- yaml::yaml.load_file(file.path(path_batch_folder, 'job_template.yaml'))
   yaml_params <- yaml_template
   
   # Modify parameters
   # yaml_params$params$param_1 <- df_combinations[r, 'param_1']
   yaml_params$params <- df_combinations[r, ]
   
   print(yaml_params$params)

   # Generate job filename ---------------------------------------------------
   
   # Generate a job filename description using UUID (without "-")
   # Create the output filename from parameters and the string template
   jobfile_name <- make_file_name(df_combinations[r, ])
   
   # Set the name in YAML job file
   yaml_params$job$job_name <- jobfile_name
   
   jobfile_name_full <- file.path(path_jobs, paste0(jobfile_name, '.yaml'))
   
   cat(sprintf('Making job "%s"\n', jobfile_name))

   if (file.exists(jobfile_name_full)) {
      stop(paste('File', jobfile_name_full, ' already exists!'))
   }
   write(as.yaml(yaml_params), jobfile_name_full)
   
   cat(sprintf('Wrote jobfile "%s".\n', jobfile_name_full))
}
