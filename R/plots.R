
#' Timeseries plot
#'
#' Converts the given parameter to a timeseries
#' and plots with `dygraphs`
#'
#' @param data A dataframe
#' @param field Character string of name of column.
#' @param timestamp Character string of name timestamp column.
#' @param title Character string of plot title.
#'
#' @return Timeseries plot
#' @export
#'
plot_ts <- function(data, field, timestamp = "timestamp", title = field) {
  field_ts <- xts::as.xts(data[[field]], data[[timestamp]])
  dygraphs::dygraph(field_ts, main = title) |>
    dygraphs::dyOptions(useDataTimezone = TRUE) |>
    dygraphs::dyRangeSelector()
}



