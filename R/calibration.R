# Functions for calibrating LECS data


#' Fit a ph model to the given data and return coefficients
#'
#' @param seaphox_data a seaphox dataframe
#' @param adv_data an adv dataframe
#'
#' @return A named vector of lm coefficents
#' @export
#' @import data.table
generate_ph_model <- function(seaphox_data, lecs_data) {
  # extract lecs data from seaphox deployment time
  lecs_data_filt <- lecs_data |>
    select(time, temp, ph_counts) |>
    filter(time > min(seaphox_data$time),
           time < max(seaphox_data$time)) |>
    dplyr::collect()

  # aggregate seaphox to nearest minute
  sp_m <- seaphox_data %>%
    mutate(time = clock::date_group(time, "minute")) |>
    dplyr::group_by(time) |>
    dplyr::summarise(seaphox_ph = mean(pH),
                     seaphox_temp = mean(temp))

  # aggregate lecs to nearest minute
  lecs_m <- lecs_data_filt  %>%
    mutate(time = clock::date_group(time, "minute")) |>
    dplyr::group_by(time) |>
    dplyr::summarise(lecs_ph_counts = mean(ph_counts),
                     lecs_temp = mean(temp))

  joined_data <- dplyr::inner_join(sp_m, lecs_m)
  # rolling join to combine lecs and seaphox data
  # joined_data <- data.table(adv_data_filt)[data.table(sp_m),
  #                                           on = .(time),
  #                                           roll = TRUE]

  ph_lm <- lm(seaphox_ph ~ lecs_ph_counts + lecs_temp, data = joined_data)
  coef(ph_lm)
}

#' calibrate pH with fit parameters
#'
#' @param ph_counts A vector of ph counts
#' @param temp A vector of temps
#' @param ph_fit A named vector of lm coeffients
#'
#' @return A vector of calibrated pH
#' @export
fit_ph_model <- function(ph_counts, temp, ph_fit) {
  ph_fit[1] + ph_fit[2]*ph_counts + ph_fit[3]*temp
}


#' Read seaphox data
#'
#' @param file seaphox file path
#'
#' @return a dataframe of seaphox data
#' @export
read_seaphox <- function(file) {
  data.table::fread(file) |>
    janitor::clean_names() |>
    mutate(time = lubridate::mdy_hms(date_time_utc_04_00,
                          tz = "UTC")) |>
    select(time,
           pH = internal_p_h_p_h,
           temp = p_h_temperature_celsius,
           pressure = pressure_decibar,
           sal = salinity_psu,
           oxygen = oxygen_ml_l)
}
