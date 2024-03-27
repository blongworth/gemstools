#' Calculate fluxes by eddy covariance method
#'
#' Calculates flux for the unit of data provided, typically hourly.
#' Run over data grouped by time period to get flux over time.
#'
#' @param quantity measured quantity
#' @param velocity measured velocity
#' @param time timestamps for quantity and velocity
#' @param frequency sampling or resampled data frequency
#' @param low_cutoff low cutoff in Hz
#' @param high_cutoff high cutoff in Hz
#'
#' @return the flux in units of `quantity` per m^2 per length of `time`
#' @export
calc_flux <- function(quantity, velocity, time, frequency, low_cutoff = 1/(15*60), high_cutoff = 0.125) {
  ts <- xts::xts(cbind(quantity, velocity), order.by = time, frequency = frequency)
  pf <- gsignal::cpsd(ts, fs = frequency)
  mask <- pf$freq > low_cutoff & pf$freq < high_cutoff
  pracma::trapz(pf$freq[mask], pf$cross[mask]) * 60 * 60 * 1000 # flux in unit/m2/hr
}

#' Calculate hourly fluxes
#'
#' @param adv_data Processed adv_data with temp, O2, and H+ concentrations
#' @param frequency Frequency of data (after resampling)
#'
#' @return A dataframe of hourly fluxes
#' @export
hourly_flux <- function(adv_data, frequency) {
  adv_data |>
    mutate(hour = lubridate::floor_date(timestamp, "hour")) |>
    group_by(hour) |>
    summarize(temp_flux = calc_flux(temp, w, timestamp, frequency),
           o_flux = calc_flux(O, w, timestamp, frequency),
           h_flux = calc_flux(H, w, timestamp, frequency))
}
