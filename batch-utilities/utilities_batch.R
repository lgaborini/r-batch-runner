# Utilities for batch files
#----------------------

requireNamespace('uuid')


# Detect whether the current script is being run on AWS (e.g. can save to S3)
is_AWS <- function() {
   IS_AWS <- Sys.getenv("IS_AWS")
   (IS_AWS == 'yes')
}



# Generate short UUID to use in filenames
make_uuid <- function(len=12) { substr(gsub('-', '', uuid::UUIDgenerate()), 1, len) }
