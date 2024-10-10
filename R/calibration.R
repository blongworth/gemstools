# Functions for calibrating LECS data

#' Convert Percent Oxygen Saturation to Concentration in μmol/L
#'
#' This function converts percent oxygen saturation to concentration in μmol/L
#' using the gsw package to calculate oxygen solubility.
#'
#' @param percent_saturation Numeric. Percent oxygen saturation (0-100).
#' @param temperature_celsius Numeric. In-situ temperature in degrees Celsius.
#' @param practical_salinity Numeric. Practical salinity (PSU).
#' @param pressure_dbar Numeric. Sea pressure in decibars.
#'
#' @return Numeric. Dissolved oxygen concentration in μmol/L.
#'
#' @importFrom gsw gsw_O2sol gsw_SA_from_SP
#'
#' @examples
#' # Example parameters
#' percent_saturation <- 85
#' temperature_celsius <- 20
#' practical_salinity <- 35
#' pressure_dbar <- 0  # surface pressure
#'
#' # Convert oxygen saturation to concentration
#' o2_sat_to_umol_l(
#'   percent_saturation, temperature_celsius, practical_salinity, pressure_dbar
#' )
#'
#' @export
o2_sat_to_umol_l <- function(percent_saturation,
                             temperature_celsius,
                             practical_salinity,
                             pressure_dbar,
                             longitude = -70,
                             latitude = 40) {
  # Convert practical salinity to absolute salinity
  absolute_salinity <- gsw_SA_from_SP(practical_salinity,
                                      pressure_dbar,
                                      longitude,
                                      latitude)

  #Conservative temp
  conservative_temp = gsw_CT_from_t(SA = absolute_salinity,
                     t = temperature_celsius,
                     p = pressure_dbar)

  # Calculate oxygen solubility in μmol/kg
  o2_solubility <- gsw_O2sol(SA = absolute_salinity,
                             CT = conservative_temp,
                             p = pressure_dbar,
                             longitude,
                             latitude)

  # Calculate density to convert from per kg to per L
  density_kg_m3 <- gsw_rho(SA = absolute_salinity,
                           CT = conservative_temp,
                           p = pressure_dbar)

  # Convert solubility from μmol/kg to μmol/L
  o2_solubility_umol_L <- o2_solubility * (density_kg_m3 / 1000)

  # Calculate actual concentration based on percent saturation
  o2_concentration_umol_L <- (percent_saturation / 100) * o2_solubility_umol_L

  return(o2_concentration_umol_L)
}

#' Convert Oxygen Concentration from ml/L to μmol/L
#'
#' This function converts dissolved oxygen concentration from milliliters per liter (ml/L)
#' to micromoles per liter (μmol/L). It accounts for temperature effects on the molar volume of oxygen.
#'
#' @param oxygen_ml_L Numeric. Dissolved oxygen concentration in ml/L.
#' @param temperature_celsius Numeric. Water temperature in degrees Celsius.
#'
#' @return Numeric. Dissolved oxygen concentration in μmol/L.
#'
#' @examples
#' # Example parameters
#' oxygen_ml_L <- 5.0
#' temperature_celsius <- 25.0
#'
#' # Convert oxygen concentration
#' o2_ml_l_to_umol_l(oxygen_ml_L, temperature_celsius)
#'
#' @export
o2_ml_l_to_umol_l <- function(oxygen_ml_L, temperature_celsius) {
  # Constants
  molar_volume_O2_STP <- 22.391  # L/mol, at STP (0°C, 1 atm)

  # Correct molar volume for temperature
  molar_volume_corrected <- molar_volume_O2_STP * (273.15 + temperature_celsius) / 273.15

  # Convert ml/L to μmol/L
  oxygen_umol_L <- (oxygen_ml_L / molar_volume_corrected) * 1e3

  return(oxygen_umol_L)
}

#' Convert Dissolved Oxygen from ml/L to μmol/kg
#'
#' This function converts dissolved oxygen concentration from milliliters per liter (ml/L)
#' to micromoles per kilogram (μmol/kg) using seawater density calculated with the gsw package.
#'
#' @param oxygen_ml_L Numeric. Dissolved oxygen concentration in ml/L.
#' @param absolute_salinity Numeric. Absolute salinity in g/kg.
#' @param temperature_celsius Numeric. In-situ temperature in degrees Celsius.
#' @param pressure_dbar Numeric. Sea pressure in decibars.
#'
#' @return Numeric. Dissolved oxygen concentration in μmol/kg.
#'
#' @importFrom gsw gsw_rho gsw_CT_from_t
#'
#' @examples
#' # Example parameters
#' oxygen_ml_l <- 5.0
#' absolute_salinity <- 35.0
#' temperature_celsius <- 25.0
#' pressure_dbar <- 0
#'
#' # Convert oxygen concentration
#' result <- convert_oxygen_ml_l_to_umol_kg(
#'   oxygen_ml_l, absolute_salinity, temperature_celsius, pressure_dbar
#' )
#' @export
o2_ml_l_to_umol_kg <- function(oxygen_ml_l,
                                           absolute_salinity,
                                           temperature_celsius,
                                           pressure_dbar) {
  # Constants
  molar_volume_O2 <- 22.391  # L/mol, at STP (0°C, 1 atm)

  # Calculate density using gsw package
  density_kg_m3 <- gsw::gsw_rho(SA = absolute_salinity,
                           CT = gsw::gsw_CT_from_t(SA = absolute_salinity,
                                              t = temperature_celsius,
                                              p = pressure_dbar),
                           p = pressure_dbar)

  # Convert density from kg/m^3 to kg/L
  density_kg_l <- density_kg_m3 / 1000

  # Correct molar volume for temperature
  molar_volume_corrected <- molar_volume_O2 * (273.15 + temperature_celsius) / 273.15

  # Convert ml/L to μmol/L
  oxygen_umol_l <- (oxygen_ml_l / molar_volume_corrected) * 1e3

  # Convert μmol/L to μmol/kg using density
  oxygen_umol_kg <- oxygen_umol_l / density_kg_l

  return(oxygen_umol_kg)
}

#' Convert O2 saturation to concentration in umol/kg
#'
#' Uses GSW. Assume sal, pressure, lon, lat if not provided.
#'
#' @param do_percent Calibrated DO saturation percent
#' @param temp Conservative temp in deg C
#' @param salinity Absolute salinity in g/kg
#' @param sea_pressure Absolute pressure - 10.1325 dbar
#' @param lon longitude in decimal degrees
#' @param lat latitude in decimal degrees
#'
#' @return O2 concentration in umol/kg
#' @export
o2_sat_to_conc <- function(do_percent,
                           temp,
                           salinity = 31.26,
                           sea_pressure = 0.2,
                           lon = -70.700833,
                           lat = 41.516944) {
  sol <- gsw::gsw_O2sol(salinity, temp, sea_pressure, lon, lat)
  do_percent / 100 * sol
}

#' Calculate H+ concentration
#'
#' @param ph Calibrated pH
#'
#' @return H+ concentration in mol/L
#' @export
pH_to_conc <- function(ph) {
  (10^(-1 * ph) * 1000) ## convert to mol/m3
}


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
    select(timestamp, temp, ph_counts) |>
    filter(timestamp > min(seaphox_data$timestamp),
           timestamp < max(seaphox_data$timestamp)) |>
    dplyr::collect()

  # aggregate seaphox to nearest minute
  sp_m <- seaphox_data %>%
    mutate(timestamp = clock::date_group(timestamp, "minute")) |>
    dplyr::group_by(timestamp) |>
    dplyr::summarise(seaphox_ph = mean(pH),
                     seaphox_temp = mean(temp))

  # aggregate lecs to nearest minute
  lecs_m <- lecs_data_filt  %>%
    mutate(timestamp = clock::date_group(timestamp, "minute")) |>
    dplyr::group_by(timestamp) |>
    #dplyr::group_by(year, month, day, hour, min) |>
    #mutate(timestamp = clock::date_time_build(year, month, day, hour, min, 0,
    #                                          zone = "America/New_York")) |>
    dplyr::summarise(timestamp = mean(timestamp),
                     lecs_ph_counts = mean(ph_counts),
                     lecs_temp = mean(temp))

  joined_data <- dplyr::inner_join(sp_m, lecs_m, by = dplyr::join_by(timestamp))
  # rolling join to combine lecs and seaphox data
  # joined_data <- data.table(adv_data_filt)[data.table(sp_m),
  #                                           on = .(time),
  #                                           roll = TRUE]

  ph_lm <- lm(seaphox_ph ~ lecs_ph_counts + lecs_temp, data = joined_data)
  coef(ph_lm)
}

#' Fit a O2 model to the given data and return coefficients
#'
#' TODO: work with o2 concentration instead of saturation
#'
#' @param seaphox_data a seaphox dataframe
#' @param adv_data an adv dataframe
#'
#' @return A named vector of lm coefficents
#' @export
#' @import data.table
generate_o2_model <- function(seaphox_data, lecs_data) {
  # extract lecs data from seaphox deployment time
  lecs_data_filt <- lecs_data |>
    select(timestamp, DO_percent) |>
    filter(timestamp > min(seaphox_data$timestamp),
           timestamp < max(seaphox_data$timestamp)) |>
    dplyr::collect()

  # aggregate seaphox to nearest minute
  sp_m <- seaphox_data %>%
    mutate(timestamp = clock::date_group(timestamp, "minute")) |>
    dplyr::group_by(timestamp) |>
    dplyr::summarise(seaphox_o2 = mean(oxy))

  # aggregate lecs to nearest minute
  lecs_m <- lecs_data_filt  %>%
    mutate(timestamp = clock::date_group(timestamp, "minute")) |>
    dplyr::group_by(timestamp) |>
    #dplyr::group_by(year, month, day, hour, min) |>
    #mutate(timestamp = clock::date_time_build(year, month, day, hour, min, 0,
    #                                          zone = "America/New_York")) |>
    dplyr::summarise(lecs_DO_percent = mean(DO_percent))

  joined_data <- dplyr::inner_join(sp_m, lecs_m, by = dplyr::join_by(timestamp))

  o2_lm <- lm(seaphox_o2 ~ lecs_DO_percent, data = joined_data)
  coef(o2_lm)
}
