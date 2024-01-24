#' Read LECS data from website
#'
#' Reads HTML table from LECS_data into a single column data frame
#'
#' @param start_date Date to get date from. Date object or string coerceable
#' to a date
#' @return a single column data frame
#' @export
#'
read_lecs_web <- function(start_date = NULL) {
  if ( is.null(start_date) ) {
    start_date = Sys.Date()
  }
  base_url <- "https://gems.whoi.edu/LECS_data/?timestamp="
  query_url <- paste0(base_url, format(as.Date(start_date), "%Y%m%d%H"))
  xml2::read_html(query_url) |>
    rvest::html_node("table") |>
    rvest::html_table()
}
