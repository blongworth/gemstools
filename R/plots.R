
#' Timeseries plot
#'
#' Converts the given parameter to a timeseries
#' and plots with `dygraphs`
#'
#' @param data A dataframe
#' @param field Character string of name of column.
#' @param timestamp Character string of name timestamp column.
#'
#' @return Timeseries plot
#' @export
#'
plot_ts <- function(data, field, timestamp = "timestamp", title = field, GMT = TRUE) {
  field_ts <- xts::as.xts(data[[field]], data[[timestamp]])
  dygraphs::dygraph(field_ts, main = title) |>
    dygraphs::dyOptions(useDataTimezone = TRUE) |>
    dygraphs::dyRangeSelector()
}

# plot a field without TS
#ggplot(df, aes(1:nrow(df), pH)) +
#  geom_scattermore()
