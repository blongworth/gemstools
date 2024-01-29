#' Read LECS data from website
#'
#' Reads HTML table from LECS_data into a single column data frame
#'
#' @param start_date Date to get date from. Date object or string coerceable
#' to a date
#' @return a single column data frame
#' @export
#'
lecs_read_web <- function(start_date = NULL) {
  if ( is.null(start_date) ) {
    start_date = Sys.Date()
  }
  base_url <- "https://gems.whoi.edu/LECS_data/?timestamp="
  query_url <- paste0(base_url, format(as.Date(start_date), "%Y%m%d%H"))
  df <- xml2::read_html(query_url) |>
    rvest::html_node("table") |>
    rvest::html_table()
  if (!nrow(df)) stop("No rows returned.")
  df
}

#' Read LECS data from web and parse into dataframes
#'
#' @param start_date Date to get date from. Date object or string coerceable
#' to a date
#'
#' @return a list containing LECS post_times, met data, status data, and ADV data
#' @export
#'
lecs_read_parse_web <- function(start_date = NULL) {
  df <- lecs_read_web(start_date) |>

    lecs_add_metadata()
  post_times <- lecs_post_times(df)
  met <- lecs_met_data(df)
  status <- lecs_status_data(df)
  adv_data <- lecs_adv_data(df, rinko_cals) |>
    make_lecs_ts(status)

  list(post_times = post_times,
       met = met,
       status = status,
       adv_data = adv_data)
}
