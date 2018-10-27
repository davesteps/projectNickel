

filename <- function(ext,level='min'){
  now <- lubridate::now(tzone = "UTC")
  switch(level,
         day=paste0(as.Date(now),ext),
         hour=paste0(as.Date(now),'-',lubridate::hour(now),ext),
         min=paste0(as.Date(now),'-',lubridate::hour(now),'-',lubridate::minute(now),ext))
}

#' bucketKeys
#'
#' @param bucket
#' @param ext
#'
#' @return
#' @export
#'
#' @examples
bucketKeys <- function(bucket,ext='bz2'){
  # given a bucket and extension
  # returns data frame with object keys and datetimes
  try({
    bl <- aws.s3::get_bucket(bucket)
    df <- plyr::ldply(bl,function(b){
      data.frame(name=b$Key,
                 time=(b$LastModified),
                 stringsAsFactors = F)})
    df$time <- as.POSIXct(df$time,format = '%Y-%m-%dT%H:%M:%S',tz = 'UTC')
    df

  },silent = T)
}

is.err <- function(x) inherits(x,"try-error")

#' sendToS3
#'
#' @param obj
#' @param bucket
#' @param tryN
#'
#' @return
#' @export
#'
#' @examples
sendToS3 <- function(obj,bucket,tryN = 5){

  put <- try(aws.s3::put_object(obj,bucket = bucket),silent = T)

  i <- 0
  while(is.err(put) & i<tryN){
    # flog.warn('No rows in data.frame')
    Sys.sleep(i)
    put <- try(aws.s3::put_object(obj,bucket = bucket),silent = T)
    i <- i+1
  }
  put
}

startLogging <- function(projDir,name,log.threshold){
  # log_file <- 'example.log'
  # logger.options()
  require(futile.logger)
  ld <- file.path(projDir,'logs')
  if(!dir.exists(ld)) dir.create(ld)
  lf <- file.path(ld,paste0(name,'-',filename('.log','day')))
  flog.appender(appender.tee(lf))
  flog.threshold(log.threshold)
}

setEnv <- function(keys){
  Sys.setenv("AWS_ACCESS_KEY_ID" = keys$AWS_ACCESS_KEY_ID,
             "AWS_SECRET_ACCESS_KEY" = keys$AWS_SECRET_ACCESS_KEY,
             "AWS_DEFAULT_REGION" = keys$AWS_DEFAULT_REGION)

}

#' aisToS3
#'
#' @param keys
#'
#' @return
#' @export
#'
#' @examples
aisToS3 <- function(projDir,log.threshold=4){
  require(projectNickel)
  require(aws.s3)

  startLogging(projDir,'aisToS3',log.threshold)
  flog.info('Starting AIShub API call')

  keys <- readRDS(file.path(projDir,'keys.Rdata'))
  setEnv(keys)

  fn <- filename('.bz2','min')
  on.exit(file.remove(fn))

  f <- try(download.file(keys$AISHUB_URL,destfile = fn))
  ct <- 1
  while (is.err(f) & ct < 5){
    flog.warn(paste('Failed to fetch file retry:',ct))
    f <- try(download.file(keys$AISHUB_URL,destfile = fn))
  }

  if(is.err(f)){
    flog.error('Failed to fetch file')
    return()
  }


  fs <- round(file.info(fn)$size/1e6,1)
  if(fs<0.2){
    flog.error(paste0('File size is too small (',fs,'MB). Not Uploading to S3.'))
    return()
  } else {
    flog.info(paste('Created',fs,'MB file.'))
  }

  put <- sendToS3(fn,'ais-current',5)

  if(is.err(put)){
    flog.error('Failed to upload to s3')
  } else {
    flog.info(paste('Successfully uploaded', fn, 'to ais-current'))
  }

}

#' aggregateAIS
#'
#' @param keys
#'
#' @return
#' @export
#'
#' @examples
aggregateAIS <- function(projDir,log.threshold=4){
  require(projectNickel)
  require(dplyr)
  require(aws.s3)

  startLogging(projDir,'aggregateAIS',log.threshold)

  keys <- readRDS(file.path(projDir,'keys.Rdata'))

  setEnv(keys)

  kl <- bucketKeys('ais-current','.bz2')

  if(is.err(kl) || !nrow(kl)){
    flog.error('Failed to retrieve any keys from ais-current')
    return()
  }

  # only get keys from last 61mins
  cutoff <- lubridate::now(tzone = "UTC") - (61*60)
  kl <- kl[kl$time > cutoff,]

  if(!nrow(kl)){
    flog.warn('No keys were found from the last hour ais-current')
    return()
  }

  kl <- kl$name
  flog.info(paste('Aggregating',length(kl), 'files'))

  fetchFile <- function(k) try({
      suppressMessages(s3read_using(readr::read_csv,object=k,bucket='ais-current'))
    })

  tl <- lapply(kl,fetchFile)

  is.valid <- function(t) if(is.err(t) || ncol(t)!=19 || !nrow(t)) F else T

  # filter out errors and (remove from kl)
  valid <- unlist(lapply(tl, is.valid))
  if(any(!valid)){
    flog.warn(paste(sum(!valid),'out of',length(kl), 'files were not valid'))
  }
  kl <- kl[valid]
  tl <- tl[valid]

  br <- try(bind_rows(tl) %>% unique(),silent = T)

  # if br is not err carry on
  if(is.err(br) || !nrow(br)){
    flog.error('Failed to aggregate files')
    return()
  }

  fn <- filename('.bz2')
  readr::write_csv(br,fn)
  on.exit(file.remove(fn))

  fi <- file.info(fn)

  flog.info(paste('Created file of size',round(fi$size/1e6),'MB'))

  put <- sendToS3(fn,'ais-archive',10)

  if(is.err(put)){
    flog.error('Failed to upload to s3')
  } else {
    flog.info('Successfully uploaded to ais-archive, deleting from ais-current')
    for(k in kl) delete_object(k,bucket = 'ais-current')
  }

}
