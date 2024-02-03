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
parse_data <- function(files, rinko_cals = rinko_cals) {
  data_lines <- purrr::map(files, read_data, .progress = TRUE) |>
    purrr::reduce(c) |>
    stringr::str_remove("^D:")

  readr::read_csv(I(data_lines),
                  col_names = c("count", "pressure",
                                'x_vel', 'y_vel', 'z_vel',
                                'x_amp', 'y_amp', 'z_amp',
                                'x_cor', 'y_cor', 'z_cor',
                                'ana_in', 'ana_in2', 'pH',
                                'temp', 'oxy'),
                  sep = ',') |>
    mutate(oxy = stringr::str_sub(oxy, 1, 10),
           dplyr::across(c('pressure',
                           'temp', 'oxy'),
                         as.numeric),
           dplyr::across(c('count',
                           'x_vel', 'y_vel', 'z_vel',
                           'x_amp', 'y_amp', 'z_amp',
                           'x_cor', 'y_cor', 'z_cor',
                           'ana_in', 'ana_in2', 'pH'),
                         as.integer),
           pressure = pressure / 65536 / 1000,
           temp = rinko_cals[["A"]] +
             temp * rinko_cals[["B"]] +
             temp ^ 2 * rinko_cals[["C"]] +
             temp ^ 3 * rinko_cals[["D"]],
           missing = dplyr::case_when(line == 1 | count > 255 | dplyr::lag(count) > 255 ~ NA_integer_,
                                      count > dplyr::lag(count) ~ count - 1L - dplyr::lag(count),
                                      TRUE ~ 255L + count - dplyr::lag(count)
           ))
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

  readr::read_csv(I(met_lines),
                  col_names = c("hour", "min", "sec", "day", "month", "year",
                                "par", "wind_speed", "wind_dir")) |>
    mutate(wind_dir = stringr::str_sub(wind_dir, 1, 6),
           across(c('hour', 'min', 'sec',
                    'day', 'month', 'year'), as.integer),
           across(c('PAR', 'wind_speed', 'wind_dir'), as.numeric),
           wind_speed = ifelse(wind_speed < 99, wind_speed, NA),
           wind_dir = ifelse(wind_dir < 360, wind_dir, NA),
           timestamp = lubridate::make_datetime(year, month, day,
                                                hour, min, sec,
                                                tz = "America/New_York")) |>
    select(timestamp, PAR, wind_speed, wind_dir)
}

#' Read status lines from given file
#'
#' @param file file to read
#'
read_status <- function(file) {
   lines <- readLines(file)
   lines[substr(lines, 1, 2) == 'S:']
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

  readr::read_csv(I(status_lines),
                  col_names = c('hour', 'min', 'sec', 'day', 'month', 'year',
                             'adv_min', 'adv_sec', 'adv_day',
                             'adv_hour', 'adv_year', 'adv_month',
                             'bat', 'soundspeed', 'heading', 'pitch',
                             'roll', 'temp',
                             'pump_current', 'pump_voltage', 'pump_power')) |>
    mutate(pump_power = stringr::str_sub(pump_power, 1, 5),
           dplyr::across(c('hour', 'min', 'day', 'month', 'year',
                           'adv_min', 'adv_day',
                           'adv_hour', 'adv_year', 'adv_month'),
                         as.integer),
           dplyr::across(c('sec', 'adv_sec',
                           'bat', 'soundspeed', 'heading', 'pitch',
                           'roll', 'temp',
                           'pump_current', 'pump_voltage', 'pump_power'),
                         as.numeric),
           dplyr::across(c('bat', 'soundspeed', 'heading', 'pitch',
                           'roll', 'temp'),
                         ~(.x) * .1),
           timestamp = lubridate::make_datetime(year, month, day, hour, min, sec,
                                                tz = "America/New_York"),
           adv_timestamp = lubridate::make_datetime(adv_year + 2000, adv_month, adv_day,
                                                    adv_hour, adv_min, adv_sec,
                                                    tz = "America/New_York"))
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


