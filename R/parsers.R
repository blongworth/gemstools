# Make list of all files

#' Process all LECS files in a dated folder
#'
#' @param date A date to use for constructing output filenames and default file path
#' @param file_dir An optional directory to use instead of the default
#' @param out_dir An optional directory to use instead of the default
#' @param dedupe Set TRUE to remove lines with duplicate timestamps
#' @param resample Set to "second", "minute", etc. to output downsampled data
#'
#' @export
lecs_process_data <- function(date, file_dir = NULL, out_dir = NULL,
                              dedupe = FALSE, resample = FALSE) {
  if (is.null(file_dir)) {
    file_dir <- paste0("data/SD Card Data/LECS_surface_sd/lecs_surface_", date)
  }
  if (is.null(out_dir)) {
    out_dir <- ""
  }

  files <- list.files(file_dir, pattern = "^202[3|4]", full.names = TRUE)
  message(paste(length(files), "files to process"))

  # Process files into a list containing data frames for ADV, status, and Met

  future::plan(future::multicore)

  tictoc::tic("Time to read and process data: ")
  lecs_data <- lecs_parse_files_p(files)
  message(tictoc::toc())
  tictoc::tic("Time to write csvs: ")
  data.table::fwrite(lecs_data[["met"]], paste0(out_dir, "lecs_met_", date, ".csv"))
  data.table::fwrite(lecs_data[["status"]], paste0(out_dir, "lecs_status_", date, ".csv"))
  data.table::fwrite(lecs_data[["adv_data"]], paste0(out_dir, "lecs_adv_data_", date, ".csv"))
  message(tictoc::toc())

  if (dedupe) {
    tictoc::tic("Time to write deduped csv: ")
    attach(lecs_data)
    adv_data |>
      dplyr::distinct(timestamp, .keep_all = TRUE) |>
      data.table::fwrite(paste0(out_dir, "lecs_adv_data_nodup_", date, ".csv"))
    message(tictoc::toc())
  }

  if (resample) {
    tictoc::tic("Time to write resampled csv: ")
    attach(lecs_data)
    adv_data |>
      dplyr::group_by(clock::date_group(timestamp, resample)) |>
      dplyr::summarise(dplyr::across(dplyr::everything(),
                       ~ mean(.x, na.rm = TRUE))) |>
      data.table::fwrite(paste0(out_dir, "lecs_adv_data_", resample, "_", date, ".csv"))
    message(tictoc::toc())

  }

}

#' Read LECS data from files and parse into dataframes
#'
#' @param files A list of file paths containing LECS data
#'
#' @return a list containing met data, status data, and ADV data
#' @export
lecs_parse_files <- function(files) {
  purrr::map(files, lecs_parse_file, .progress = TRUE) |>
    simplify2array() |> apply(1, purrr::list_rbind)
}

#' Parallelized Read LECS data from files and parse into dataframes
#'
#' @param files A list of file paths containing LECS data
#'
#' @return a list containing met data, status data, and ADV data
#' @export
lecs_parse_files_p <- function(files) {
  furrr::future_map(files, lecs_parse_file, .progress = TRUE) |>
    purrr::transpose() |>
    furrr::future_map(purrr::list_rbind)
}

#' Read LECS data from file and parse into dataframes
#'
#' @param file A file path in LECS data format
#'
#' @return a list containing LECS post_times, met data, status data, and ADV data
#' @export
#'
lecs_parse_file <- function(file) {
  df <- lecs_read_file(file)
  met <- lecs_met_data(df)
  status <- lecs_status_data(df)
  adv_data <- lecs_adv_data(df, rinko_cals) |>
    make_lecs_ts_2(status)

  list(met = met,
       status = status,
       adv_data = adv_data)
}

#' LECS raw file parser
#'
#' Parse raw files to a df with row_num and type
#' Remove type chars from beginning of lines
#'
#' @param files a vector of file paths
#'
#' @return a dataframe with row_num, type, and unparsed data
#' @export
#' @importFrom stringr str_sub
lecs_read_file <- function(file) {
  l <- readLines(file)
  type <- str_sub(l, 1, 1)
  data <- str_sub(l, start = 3)
  data.frame(row_num = 1:length(l), type = type, data = data)
}

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
    mutate(row_num = dplyr::row_number(),
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
    select(-row_num, -type, -any_of("line"), -hour, -min, -sec, -day, -month, -year)
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
#' @importFrom magrittr %>%
lecs_adv_data <- function(df, rinko_cals) {
  df |>
    dplyr::filter(type == "D") |>
    tidyr::separate(data,
                    into = c('count', 'pressure',
                             'u', 'v', 'w',
                             'amp1', 'amp2', 'amp3',
                             'corr1', 'corr2', 'corr3',
                             'ana_in', 'ana_in2', 'ph_counts',
                             'temp', 'DO'),
                    sep = ',') |>
    dplyr::filter(!is.na(count),
                  count >= 0,
                  count < 256) %>%
    mutate(DO = stringr::str_sub(DO, 1, 10),
           dplyr::across(c('pressure',
                           'temp', 'DO'),
                         as.numeric),
           dplyr::across(c('count',
                             'u', 'v', 'w',
                             'amp1', 'amp2', 'amp3',
                             'corr1', 'corr2', 'corr3',
                           'ana_in', 'ana_in2', 'ph_counts'),
                         as.integer),
           dplyr::across(c('u', 'v', 'w'), ~(.x) * .001),
           pressure = pressure / 65536 / 1000,
           temp = rinko_cals[["temp_A"]] +
             temp * rinko_cals[["temp_B"]] +
             temp ^ 2 * rinko_cals[["temp_C"]] +
             temp ^ 3 * rinko_cals[["temp_D"]],
           DO_percent = (( rinko_cals[["o2_A"]]) /( 1 + rinko_cals[["o2_D"]]*(temp-25))) +
            ((rinko_cals[["o2_B"]]) / ((DO - rinko_cals[["o2_F"]])*(1+ rinko_cals[["o2_D"]]*(temp-25)) +
             rinko_cals[["o2_C"]] + rinko_cals[["o2_F"]])) * rinko_cals[["o2_H"]] +
              rinko_cals[["o2_G"]]
    )
}

#' Calculate number of missing lines
#'
#' @param count Vector of ADV count
#' @param line vector indicating line of send
#'
#' @return A vector with a guess at the number of missing lines from
#' previous to current
#' @export
lecs_missing <- function(count, line = NULL) {
  missing <-  dplyr::case_when(count > 255 | dplyr::lag(count) > 255 ~
                                 NA_integer_,
                               count > dplyr::lag(count) ~
                                 count - 1L - dplyr::lag(count),
                               TRUE ~
                                 255L + count - dplyr::lag(count))
  if (!is.null(line)) {
    missing = replace(missing, line == 1, NA)
  }
  missing
}
