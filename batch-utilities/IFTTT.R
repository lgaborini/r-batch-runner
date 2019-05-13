requireNamespace('httr')

IFTTT_notify <- function(value1 = NULL, value2 = NULL, value3 = NULL, IFTTT_R_event = 'R_finished') {

   IFTTT_key <- Sys.getenv('IFTTT_key')
   if (IFTTT_key == '') {
      return(invisible(NULL))
   }
   
   IFTTT_URL <- sprintf('https://maker.ifttt.com/trigger/%s/with/key/%s', IFTTT_R_event, IFTTT_key)
   # Do not throw on errors
   r <- tryCatch(httr::POST(IFTTT_URL, body = list(value1 = value1, value2 = value2, value3 = value3)), error = invisible)
}

# Test:
# IFTTT_notify('test')
