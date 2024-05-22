#' Calculate fluxes by eddy covariance method
#'
#' Calculates flux for the unit of data provided, typically hourly.
#' Run over data grouped by time period to get flux over time.
#'
#' @param quantity measured quantity
#' @param velocity measured velocity
#' @param time timestamps for quantity and velocity
#' @param window length of data in seconds
#' @param frequency sampling or resampled data frequency
#' @param low_cutoff low cutoff in Hz
#' @param high_cutoff high cutoff in Hz
#'
#' @return the flux in units of `quantity` per m^2 per length of `time`
#' @export
calc_flux <- function(quantity, velocity, time,
                      frequency,
                      window = 30 * 60, # 30 min window
                      low_cutoff = 1/(15*60),
                      high_cutoff = 0.125) {
  win <- max(length(quantity), window %/% (1/frequency))
  ts <- xts::as.xts(cbind(velocity, quantity), order.by = time, frequency = frequency)
  pf <- gsignal::cpsd(ts, fs = frequency, window = win, overlap = 0)
  mask <- pf$freq > low_cutoff & pf$freq < high_cutoff
  flux <-  pracma::trapz(pf$freq[mask], pf$cross[mask])
  flux * 60 * 60 # convert flux per second to flux per hour
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
