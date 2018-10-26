

filename <- function(ext,level){
  now <- lubridate::now(tzone = "UTC")
  paste0(as.Date(now),'-',lubridate::hour(now),'-',lubridate::minute(now),ext)
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


#' aisToS3
#'
#' @param keys
#'
#' @return
#' @export
#'
#' @examples
aisToS3 <- function(keys){

  Sys.setenv("AWS_ACCESS_KEY_ID" = keys$AWS_ACCESS_KEY_ID,
             "AWS_SECRET_ACCESS_KEY" = keys$AWS_SECRET_ACCESS_KEY,
             "AWS_DEFAULT_REGION" = keys$AWS_DEFAULT_REGION)


  fn <- filename('.bz2')
  download.file(keys$AISHUB_URL,destfile = fn)
  on.exit(file.remove(fn))

  sendToS3(fn,'ais-current',5)

}

#' aggregateAIS
#'
#' @param keys
#'
#' @return
#' @export
#'
#' @examples
aggregateAIS <- function(keys){

  Sys.setenv("AWS_ACCESS_KEY_ID" = keys$AWS_ACCESS_KEY_ID,
             "AWS_SECRET_ACCESS_KEY" = keys$AWS_SECRET_ACCESS_KEY,
             "AWS_DEFAULT_REGION" = keys$AWS_DEFAULT_REGION)

  kl <- bucketKeys('ais-current','.bz2')

  if(is.err(kl) || !nrow(kl)) return()

  # only get keys from last 61mins
  cutoff <- lubridate::now(tzone = "UTC") - (61*60)
  kl <- kl[kl$time > cutoff,]

  if(!nrow(kl)) return()

  kl <- kl$name

  tl <- lapply(kl,function(k) try(s3read_using(readr::read_csv,object=k,bucket='ais-current'),silent = T))

  # filter out errors and (remove from kl)
  errors <- unlist(lapply(tl, is.err))
  kl <- kl[!errors]
  tl <- tl[!errors]

  br <- try(bind_rows(tl) %>% unique(),silent = T)

  # if br is not err carry on
  if(is.err(br)) return()

  fn <- filename('.bz2')
  readr::write_csv(br,fn)
  on.exit(file.remove(fn))
  put <- sendToS3(fn,'ais-archive',10)
  if(!is.err(put)) for(k in kl) aws.s3::delete_object(k,bucket = 'ais-current')

}
