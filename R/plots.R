
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

#' Plot timeseries pointrange with mean and error
#'
#' @param data A dataframe with `timestamp_mean`
#' @param parameter Parameter to plot.
#' @param err Error in parameter
#'
#' @return A ggplot object
#' @export
plot_mean <- function(data, parameter, err) {
  data |>
    ggplot2::ggplot(aes(x = timestamp_mean,
               y = {{parameter}},
               ymin = {{parameter}} - {{err}},
               ymax = {{parameter}} + {{err}})) +
    ggplot2::geom_line(color = "gray") +
    ggplot2::geom_pointrange() +
    ggplot2::labs(x = NULL)
}



