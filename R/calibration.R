# Functions for calibrating LECS data

#' calibrate pH with fit parameters
#'
#' @param ph_counts A vector of ph counts
#' @param temp A vector of temps
#' @param ph_fit A named vector of lm coefficents (int, ph, temp)
#'
#' @return A vector of calibrated pH
#' @export
cal_ph <- function(ph_counts, temp, ph_fit) {
  stopifnot(names(ph_fit) == c("int", "ph", "temp") |
            names(ph_fit) == c("(Intercept)", "lecs_ph_counts", "lecs_temp") )
  ph_fit[1] + ph_fit[2]*ph_counts + ph_fit[3]*temp
}

#' Calibrate Rinko temp
#'
#' @param raw_temp A vector of raw temp voltage measurements
#' @param rinko_cals A named vector of rinko calibration coefficents
#'
#' @return A vector of calibrated temperatures
#' @export
cal_temp <- function(raw_temp, rinko_cals) {
  rinko_cals["temp_A"] +
    raw_temp * rinko_cals["temp_B"] +
    raw_temp ^ 2 * rinko_cals["temp_C"] +
    raw_temp ^ 3 * rinko_cals["temp_D"]
}

#' Calibrate Rinko Oxygen saturation
#'
#' @param raw_do A vector of raw dissolved oxygen voltage measurements
#' @param rinko_cals A named vector of rinko calibration coefficents
#'
#' @return A vector of calibrated oxygen saturation in percent
#' @export
cal_ox <- function(raw_do, temp, rinko_cals) {
  (( rinko_cals["o2_A"]) / ( 1 + rinko_cals["o2_D"] * (temp - 25))) +
    ((rinko_cals["o2_B"]) /
       ((raw_do - rinko_cals["o2_F"]) * (1 + rinko_cals["o2_D"] * (temp - 25)) +
          rinko_cals["o2_C"] + rinko_cals["o2_F"])) * rinko_cals["o2_H"] +
    rinko_cals["o2_G"]
}

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

