
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
           type = str_match(X1, "\\[\\d+\\]([DMS$!]):?")[,2],
           send = cumsum(type == "$"),
           line = as.integer(str_match(X1, "\\[(\\d+)\\][DMS$!]:?")[,2]),
           data = str_remove(X1, "\\[\\d+\\][DMS$!]:?")) |> 
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
    filter(type == "$") |> 
    separate(data, 
             into = c('hour', 'min', 'sec', 
                      'day', 'month', 'year',
                      'lat', 'lon'),
             sep = ',') |>
    mutate(across(5:10, as.integer),
           across(11:12, as.numeric),
           timestamp = make_datetime(year, month, day, 
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
    filter(type == "M") |> 
    separate(data, 
             into = c('hour', 'min', 'sec', 
                      'day', 'month', 'year', 
                      'PAR', 'wind_speed', 'wind_dir'), 
             sep = ',') |> 
    mutate(wind_dir = str_sub(wind_dir, 1, 6),
           across(5:10, as.integer),
           across(11:13, as.numeric),
           wind_speed = ifelse(wind_speed < 99, wind_speed, NA),
           wind_dir = ifelse(wind_dir < 360, wind_dir, NA),
           timestamp = make_datetime(year, month, day, 
                                     hour, min, sec, 
                                     tz = "America/New_York")) |> 
    filter(timestamp > "2023-01-01",
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
    filter(type == "S") |> 
    separate(data, 
             into = c('hour', 'min', 'sec', 'day', 'month', 'year',
                      'adv_min', 'adv_sec', 'adv_day', 
                      'adv_hour', 'adv_year', 'adv_month',
                      'bat', 'soundspeed', 'heading', 'pitch', 
                      'roll', 'temp', 
                      'pump_current', 'pump_voltage', 'pump_power'),
             sep = ',') |>
    mutate(pump_power = str_sub(pump_power, 1, 5),
           across(5:16, as.integer),
           across(17:25, ~ as.numeric(.x) * .1),
           timestamp = make_datetime(year, month, day, hour, min, sec,
                                     tz = "America/New_York"),
           adv_timestamp = make_datetime(adv_year + 2000, adv_month, adv_day, 
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
    filter(type == "D") |> 
    separate(data, 
             into = c('count', 'pressure', 
                      'x_vel', 'y_vel', 'z_vel',
                      'x_amp', 'y_amp', 'z_amp',
                      'x_cor', 'y_cor', 'z_cor',
                      'ana_in', 'ana_in2', 'pH', 
                      'temp', 'oxy'),
             sep = ',') |>
    mutate(oxy = str_sub(oxy, 1, 10),
           across(6:20, as.numeric),
           count = as.integer(count),
           pressure = pressure / 65536 / 1000,
           temp = rinko_cals[["A"]] + 
                  temp * rinko_cals[["B"]] + 
                  temp ^ 2 * rinko_cals[["C"]] + 
                  temp ^ 3 * rinko_cals[["D"]],
           missing = case_when(line == 1 | count > 255 | lag(count) > 255 ~ NA_integer_,
                               count > lag(count) ~ count - 1L - lag(count),
                               TRUE ~ 255L + count - lag(count)
                             ))
}