


#' latestAIS
#'
#' @param projDir
#'
#' @return
#' @export
#'
#' @examples
latestAIS <- function(projDir='~/ais'){

  keys <- readRDS(file.path(projDir,'keys.Rdata'))

  setEnv(keys)

  kl <- bucketKeys('ais-current','.bz2')
  kl <- tail(kl[order(kl$time),],1)

  suppressMessages(aws.s3::s3read_using(readr::read_csv,object=kl$name,bucket='ais-current'))

}


