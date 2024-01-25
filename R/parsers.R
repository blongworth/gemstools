# ADV Status
#col_types = c(day = 'i', month = 'i', 'year', 'hour', 'min', 'sec', 'error', 'status', 'X9', 'X10', 'X11', 'X12', 'X13', 'X14', 'X15', 'X16'))

#ts_offset <- 671628945

#' Parse ADV status file
#'
#' @param file ADV .sen status file
#' @param ts_offset time to add to correct timestamp in seconds
#'
#' @return A data frame of status data
#' @export
#'
read_adv_status <- function(file, ts_offset = 0) {
  readr::read_fwf(file) |>
    dplyr::mutate(across(X1:X6, as.integer),
      ts = lubridate::make_datetime(year = X3, month = X1, day = X2,
                              hour = X4, min = X5, sec = X6,
                              tz = "EDT"),
           timestamp = ts + ts_offset,
           bat = X9,
           temp = X14)
}

# concatenate LECS SD data files

#' Read data lines from given file
#'
#' @param file file to read
#'
read_data <- function(file) {
   lines <- readLines(file)
   lines[grep('^D', lines)]
}

#' Parse ADV Data from MCU SD card files
#'
#' @param files a list of data files
#'
#' @return A data frame of ADV data
#' @export
parse_data <- function(files) {
  data_lines <- purrr::map(files, read_data, .progress = TRUE) |>
    purrr::reduce(c) |>
    stringr::str_remove("^D:")

  readr::read_csv(I(data_lines),
           col_names = c("count", "pressure",
                         "vx", "vy", "vz",
                         "a1", "a2", "a3",
                         "corr1", "corr2", "corr3",
                         "ana_in", "ana_in2", "pH",
                         "temp", "DO"))
}

#' Read met lines from given file
#'
#' @param file file to read
#'
read_met <- function(file) {
   lines <- readLines(file)
   lines[grep('M:', lines)]
}

#' Parse met Data from MCU SD card files
#'
#' @param files a list of data files
#'
#' @return A data frame of met data
#' @export
parse_met <- function(files) {
  met_lines <- purrr::map(files, read_met, .progress = TRUE) |>
    purrr::reduce(c) |>
    stringr::str_remove("^.*M:")

  readr::read_csv(I(met_lines), col_names = c("hour", "min", "sec", "day", "month", "year",
                                       "par", "wind_speed", "wind_dir")) |>
    dplyr::filter(year == 2023) |>
    dplyr::mutate(timestamp = lubridate::make_datetime(year = year, month = month, day = day,
                              hour = hour, min = min, sec = sec))
}

#' Read status lines from given file
#'
#' @param file file to read
#'
read_status <- function(file) {
   lines <- readLines(file)
   lines[grep('S:', lines)]
}

#' Parse status Data from MCU SD card files
#'
#' @param files a list of data files
#'
#' @return A data frame of ADV status data
#' @export
parse_status <- function(files) {
  status_lines <- purrr::map(files, read_status, .progress = TRUE) |>
    purrr::reduce(c) |>
    stringr::str_remove("^.*S:")

  readr::read_csv(I(status_lines), col_names = c("hour", "min", "sec", "day", "month", "year",
                                          "adv_min", "adv_sec", "adv_day", "adv_hour", "adv_year", "adv_month",
                                          "bat", "ss", "head", "pitch", "roll", "temp",
                                          "empty", "CR", "BV", "PWR")) |>
    dplyr::filter(year == 2023) |>
    dplyr::mutate(timestamp = lubridate::make_datetime(year = year, month = month, day = day,
                              hour = hour, min = min, sec = sec),
           bat = bat * .1)
}

#' Read status messages from given file
#'
#' @param file file to read
#'
read_statusmsg <- function(file) {
   lines <- readLines(file)
   lines[grep('^!', lines)]
}


#' Parse status messages from MCU SD card files
#'
#' @param files a list of data files
#'
#' @return A data frame of status messages
#' @export
parse_statusmsg <- function(files) {
  statusmsg_lines <- purrr::map(files, read_statusmsg, .progress = TRUE) |>
    purrr::reduce(c) |>
    stringr::str_remove("^!")

  readr::read_csv(I(statusmsg_lines), col_names = c("hour", "min", "sec", "day", "month", "year", "message")) |>
    dplyr::filter(year == 2023) |>
    dplyr::mutate(timestamp = lubridate::make_datetime(year = year, month = month, day = day,
                              hour = hour, min = min, sec = sec))
}


