
#' api_call
#'
#' @param api_info
#' @param out_dir
#'
#' @return
#' @export
#'
#' @examples
api_call <- function(out_dir,api_info=''){
  df <- fetchData(api_info)
  df <- cleanData(df)
  writeData(df,out_dir)
}

#' fetchData
#'
#' @param api_info
#'
#' @return
#' @export
#'
#' @examples
fetchData <- function(api_info=''){
  iris
}

#' cleanData
#'
#' @return
#' @export
#'
#' @examples
cleanData <- function(df){
  df
}

#' writeData
#'
#' @param dir
#'
#' @return
#' @export
#'
#' @examples
writeData <- function(df,out_dir='.'){
  # df <- iris
  fn <- file.path(out_dir,paste0(Sys.Date(),'.csv'))
  new_file <- ifelse(file.exists(fn),F,T)
  write.table(df,fn,append = new_file,col.names = F,row.names = F)
}


