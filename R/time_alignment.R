# Functions for adding timestamps to ADV data lines

#' Apply timestamps to LECS ADV data
#'
#' Dplyr version from June Choe (github.com/yjunechoe)
#'
#' @param adv_data A dataframe with ADV data lines
#' @param status A dataframe with parsed timestamps and original row_nums
#'
#' @return ADV data with timestamps added
#' @export
#' @importFrom dplyr filter select mutate
make_lecs_ts <- function(adv_data, status) {
  adv_data |>
    mutate(timestamp = as.POSIXct(NA)) |>
    dplyr::bind_rows(select(status, row_num, type, timestamp)) |>
    dplyr::arrange(row_num) |>
    mutate(
      timestamp = replace(timestamp, which(type == "S") + 1, timestamp[type == "S"])
    ) %>%
    filter(type == "D") %>%
    mutate(
      boundary = c(TRUE, abs(diff(count)) > 3), # tol = 3
      #spread_group = replace(rep(NA, dplyr::n()), which(boundary), seq_len(sum(boundary)))
      spread_group = cumsum(boundary)
    ) %>%
    #tidyr::fill(spread_group) %>%
    dplyr::group_by(spread_group) %>%
    mutate(
      difftime = (count - count[which.min(is.na(timestamp))]) * 0.0625,
      timestamp = timestamp[which.min(is.na(timestamp))] + difftime,
    ) %>%
    dplyr::ungroup() |>
    select(-c(boundary, spread_group, difftime))
}

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
make_lecs_ts_2 <- function(adv_data, status) {
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
      nextind = dplyr::if_else(is.na(timestamp), count2[NA], count2),
      prevind = nextind
    ) |>
    tidyr::fill(prevtime, prevind, .direction = "down") |>
    tidyr::fill(nexttime, nextind, .direction = "up") |>
    mutate(
      newtimestamp = dplyr::case_when(
        !is.na(timestamp) ~ timestamp,
        is.na(prevtime) | abs(count2 - nextind) < abs(count2 - prevind) ~
          nexttime + (count2 - nextind)/16,
        TRUE ~
          prevtime + (count2 - prevind)/16
      )
    ) |>
    dplyr::ungroup() |>
    filter(type == "D") |>
    select(names(adv_data), timestamp = newtimestamp)
}

#' Difference between two numbers in a circular 1 byte number
#'
#' @param c1 first number
#' @param c2 second number
#'
#' @return difference
counts_between <- function(c1, c2) {
  if (is.na(c1) | is.na(c2)) return(NA)
  if (c2 - c1 >= 0) {
    c2 - c1
  } else {
    256 + c2 - c1
  }
}

#' Determine previous status line index
#'
#' @param stat_df Status dataframe
#' @param row_num row_num of data line
#'
#' @return row_num of previous status line
prev_status <- function(stat_df, row_num) {
  prev_stats <- stat_df["row_num"][stat_df["row_num"] < row_num]
  if (is.logical(prev_stats)) { # test for no previous status line
    NA
  } else {
    max(prev_stats)
  }
}

#' Determine next status line index
#'
#' @param stat_df Status dataframe
#' @param row_num row_num of data line
#'
#' @return row_num of next status line
next_status <- function(stat_df, row_num) {
  prev_stats <- stat_df["row_num"][stat_df["row_num"] > row_num]
  if (is.logical(prev_stats)) { # test for no  status line
    NA
  } else {
    min(prev_stats)
  }
}

#' find count at status line index
#'
#' Assume first line after status is same time as status
#'
#' @param data_df adv data dataframe
#' @param status_row row_num of status row
#'
#' @return count
count_tzero <- function(data_df, status_row) {
  next_row <- suppressWarnings(min(data_df["row_num"][data_df["row_num"] > status_row]))
  ifelse(is.infinite(next_row), NA, data_df["count"][data_df["row_num"] == next_row])
}

#' Calculate D line timestamp from S line timestamps
#'
#' @param cur_count Current D line count
#' @param prev_ts Last S line timestamp
#' @param next_ts Next S line timestamp
#' @param prev_count Count at last S line timestamp
#' @param next_count Count at next S line timestamp
#' @param interval Time between D lines
#'
#' @return timestamp of current D line
calc_ts <- function(cur_count, prev_ts, next_ts, prev_count, next_count, interval = 0.0625) {
  prev_diff <- counts_between(prev_count, cur_count)
  next_diff <- counts_between(cur_count, next_count)
  if (is.na(next_diff) | is.na(prev_diff)) return(NA)
  if ( next_diff > prev_diff ) {
    prev_ts + prev_diff * interval
  } else {
    next_ts - next_diff * interval
  }
}

#' Get the timestamp for a row of adv data
#'
#' @param adv_data an adv dataframe
#' @param stat_df corresponding status dataframe
#' @param i row index
#' @param interval seconds between data lines
#'
#' @return timestamp of the current d line
#' @export
add_timestamp <- function(adv_data, stat_df, i, interval = 0.0625) {
  cur_count <- adv_data[["count"]][i]
  psr <- prev_status(stat_df, adv_data[["row_num"]][i])
  nsr <- next_status(stat_df, adv_data[["row_num"]][i])
  prev_ts <- stat_df[["timestamp"]][stat_df["row_num"] == psr]
  next_ts <- stat_df[["timestamp"]][stat_df["row_num"] == nsr]
  prev_count <- count_tzero(adv_data, psr)
  next_count <- count_tzero(adv_data, nsr)

  calc_ts(cur_count, prev_ts, next_ts, prev_count, next_count, interval)
}

#' Add timestamps to ADV data
#'
#' @param adv_data adv dataframe
#' @param stat_df adv status dataframe
#'
#' @return the adv dataframe with timestamps added
#' @export
#'
add_timestamps <- function(adv_data, stat_df) {
  adv_data$timestamp <- as.POSIXct(NA)
  for (i in 1:nrow(adv_data)) {
    adv_data$timestamp[[i]] <- add_timestamp(adv_data, stat_df, i)
  }
  adv_data
}
