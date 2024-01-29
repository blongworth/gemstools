#' Parse LECS metadata
#'
#' Add row numbers, line type, number and line of send (web data),
#'
#' @param df_raw a raw LECS dataframe
#'
#' @return a dataframe with added metadata
#' @export
lecs_add_metadata <- function(df_raw) {
  df_raw |>
    mutate(row_num = row_number(),
           type = stringr::str_match(X1, "\\[\\d+\\]([DMS$!]):?")[,2],
           send = cumsum(type == "$"),
           line = as.integer(stringr::str_match(X1, "\\[(\\d+)\\][DMS$!]:?")[,2]),
           data = stringr::str_remove(X1, "\\[\\d+\\][DMS$!]:?")) |>
    select(row_num, send, type, line, data)
}

#' parse LECS post times
#'
#' @param df a LECS dataframe with added metadata
#'
#' @return a dataframe of post times
#' @export
lecs_post_times <- function(df) {
  df |>
    dplyr::filter(type == "$") |>
    tidyr::separate(data,
             into = c('hour', 'min', 'sec',
                      'day', 'month', 'year',
                      'lat', 'lon'),
             sep = ',') |>
    mutate(across(c('hour', 'min', 'sec',
                      'day', 'month', 'year'), as.integer),
           across(c('lat', 'lon'), as.numeric),
           timestamp = lubridate::make_datetime(year, month, day,
                                     hour, min, sec,
                                     tz = "America/New_York"),
           row_count = row_num - lag(row_num)) |>
    select(timestamp, row_count)
}

#' parse LECS met data
#'
#' @param df a LECS dataframe with added metadata
#'
#' @return a dataframe of met data
#' @export
lecs_met_data <- function(df) {
  df |>
    dplyr::filter(type == "M") |>
    tidyr::separate(data,
             into = c('hour', 'min', 'sec',
                      'day', 'month', 'year',
                      'PAR', 'wind_speed', 'wind_dir'),
             sep = ',') |>
    mutate(wind_dir = stringr::str_sub(wind_dir, 1, 6),
           across(c('hour', 'min', 'sec',
                      'day', 'month', 'year'), as.integer),
           across(c('PAR', 'wind_speed', 'wind_dir'), as.numeric),
           wind_speed = ifelse(wind_speed < 99, wind_speed, NA),
           wind_dir = ifelse(wind_dir < 360, wind_dir, NA),
           timestamp = lubridate::make_datetime(year, month, day,
                                     hour, min, sec,
                                     tz = "America/New_York")) |>
    dplyr::filter(timestamp > "2023-01-01",
           timestamp < "2024-10-01") |>
    select(timestamp, send, PAR, wind_speed, wind_dir)
}

#' parse LECS status data
#'
#' @param df a LECS dataframe with added metadata
#'
#' @return a dataframe of status data
#' @export
lecs_status_data <- function(df) {
  df |>
    dplyr::filter(type == "S") |>
    tidyr::separate(data,
                    into = c('hour', 'min', 'sec', 'day', 'month', 'year',
                             'adv_min', 'adv_sec', 'adv_day',
                             'adv_hour', 'adv_year', 'adv_month',
                             'bat', 'soundspeed', 'heading', 'pitch',
                             'roll', 'temp',
                             'pump_current', 'pump_voltage', 'pump_power'),
                    sep = ',') |>
    mutate(pump_power = stringr::str_sub(pump_power, 1, 5),
           dplyr::across(c('hour', 'min', 'day', 'month', 'year',
                           'adv_min', 'adv_day',
                           'adv_hour', 'adv_year', 'adv_month'),
                         as.integer),
           dplyr::across(c('sec', 'adv_sec',
                           'bat', 'soundspeed', 'heading', 'pitch',
                           'roll', 'temp',
                           'pump_current', 'pump_voltage', 'pump_power'), as.numeric),
           dplyr::across(c('bat', 'soundspeed', 'heading', 'pitch',
                           'roll', 'temp') , ~(.x) * .1),
           timestamp = lubridate::make_datetime(year, month, day, hour, min, sec,
                                                tz = "America/New_York"),
           adv_timestamp = lubridate::make_datetime(adv_year + 2000, adv_month, adv_day,
                                                    adv_hour, adv_min, adv_sec,
                                                    tz = "America/New_York"))
}

#' parse LECS ADV data
#'
#' Run time alignment after this
#'
#' @param df a LECS dataframe with added metadata
#' @param rinko_cals a list of rinko cal factors (A-D)
#'
#' @return a dataframe of ADV data
#' @export
lecs_adv_data <- function(df, rinko_cals) {
  df |>
    dplyr::filter(type == "D") |>
    tidyr::separate(data,
                    into = c('count', 'pressure',
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
