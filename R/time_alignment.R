#' Apply timestamps to LECS ADV data
#'
#' Version from R2evans on stack overflow
#'
#' @param adv_data A dataframe with ADV data lines
#' @param status A dataframe with parsed timestamps and original row_nums
#'
#' @return ADV data with timestamps added
#' @export
#' @importFrom dplyr filter select mutate
make_lecs_ts <- function(adv_data, status) {
  if ("send" %in% names(status)) {
    df <- adv_data |>
      mutate(timestamp = as.POSIXct(NA)) |>
      dplyr::bind_rows(select(status, row_num, send, type, timestamp)) |>
      dplyr::arrange(row_num) |>
      dplyr::group_by(send)
  } else {
    df <- adv_data |>
      mutate(timestamp = as.POSIXct(NA)) |>
      dplyr::bind_rows(select(status, row_num, type, timestamp)) |>
      dplyr::arrange(row_num)
  }
  df |>
  mutate(count2 = count, nexttime = timestamp, prevtime = timestamp) |>
    tidyr::fill(count2, .direction = "updown") |>
    mutate(
      count2 = count2 + 256*cumsum(c(FALSE, diff(count2) < 0)),
      boundary = c(TRUE, abs(diff(count2)) > 3), # tol = 3
      spread_group = cumsum(boundary)
    ) |>
    dplyr::group_by(spread_group) |>
    mutate(
      nextind = dplyr::if_else(is.na(timestamp), count2[NA], count2),
      prevind = nextind,
    ) |>
    tidyr::fill(prevtime, prevind, .direction = "down") |>
    tidyr::fill(nexttime, nextind, .direction = "up") |>
    mutate(
      newtimestamp = dplyr::case_when(
        !is.na(timestamp) ~ timestamp,
        is.na(prevtime) | count2 - dplyr::lag(count2) > 2 ~
          nexttime + (count2 - nextind)/16,
        TRUE ~
          prevtime + (count2 - prevind)/16
      )
    ) |>
    dplyr::ungroup() |>
    filter(type == "D") |>
    mutate(year = clock::get_year(timestamp),
           month = clock::get_month(timestamp),
           day = clock::get_day(timestamp),
           hour = clock::get_hour(timestamp),
           min = clock::get_minute(timestamp),
           sec = clock::get_second(timestamp)) |>
    select(timestamp = newtimestamp, names(adv_data),
           year, month, day, hour, min, sec)
}
