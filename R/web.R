#' Read LECS data from website
#'
#' Reads HTML table from LECSrawdata into a single column data frame
#'
#' @return a single column data frame
#' @export
#'
read_lecs_web <- function() {
  xml2::read_html("https://gems.whoi.edu/LECSrawdata/") |>
    rvest::html_node("table") |>
    rvest::html_table()
}
