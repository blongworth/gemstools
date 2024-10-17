# Functions for processing raw LECS data

#' Process all LECS files in a dated folder
#'
#' @param date A date to use for constructing output filenames and default file path
#' @param file_dir An optional directory to use instead of the default
#' @param files An optional list of files to process. Supersedes `date` and
#' `file_dir` for generating file list
#' @param out_dir An optional directory to use instead of the default
#' @param clean Set TRUE to remove bad data and timestamps
#' @param dedupe Set TRUE to remove lines with duplicate timestamps
#' @param resample Set to "second", "minute", etc. to output downsampled data
#' @param csv Set TRUE to write csv data
#' @param parquet Set TRUE to write parquet (Arrow) data
#'
#' @export
lecs_process_data <- function(date = NULL,
                              file_dir = NULL,
                              files = NULL,
                              out_dir = ".",
                              clean = TRUE,
                              dedupe = FALSE,
                              resample = FALSE,
                              csv = FALSE,
                              parquet = TRUE
                              ) {
  if ( is.null(c(date, file_dir, files)) ) {
    stop("Provide a date, file_dir, or a list of files to process.")
  }

  if (is.null(file_dir)) {
    file_dir <- paste0("data/SD Card Data/LECS_surface_sd/lecs_surface_", date)
  }

  if (is.null(files)) {
    files <- list.files(file_dir, pattern = "^202[3|4]", full.names = TRUE)
  }

  message(paste(length(files), "files to process"))

  # Process files into a list containing data frames for ADV, status, and Met

  future::plan(future::multicore)

  tictoc::tic("Time to read and process data: ")
  lecs_data <- lecs_parse_files_p(files, clean)
  message(tictoc::toc(), "\n")

  attach(lecs_data)

  if (dedupe) {
    adv_data <- adv_data |>
      dplyr::distinct(timestamp, .keep_all = TRUE)
  }

  if (resample) {
    adv_data <- adv_data |>
      dplyr::group_by(clock::date_group(timestamp, resample)) |>
      dplyr::summarise(dplyr::across(dplyr::everything(),
                       ~ mean(.x, na.rm = TRUE)))
  }

  #if (clean) {
  #  met <- lecs_clean_met(met)
  #  status <- lecs_clean_status(status)
  #  adv_data <- lecs_clean_adv_data(adv_data)
  #}

  if (csv) {
    tictoc::tic("Time to write csvs: ")
    data.table::fwrite(met, paste0(out_dir, "lecs_met_", date, ".csv"))
    data.table::fwrite(status, paste0(out_dir, "lecs_status_", date, ".csv"))
    data.table::fwrite(adv_data, paste0(out_dir, "lecs_adv_data_", date, ".csv"))
    message(tictoc::toc(), "\n")
  }

  if (parquet) {
    tictoc::tic("Time to write parquet: ")
    dir.create(file.path(out_dir, "lecs_met.parquet"))
    dir.create(file.path(out_dir, "lecs_status.parquet"))
    dir.create(file.path(out_dir, "lecs_adv_data.parquet"))
    met |>
      arrow::arrow_table() |>
      #dplyr::group_by(year, month) |>
      arrow::write_dataset(paste0(out_dir, "lecs_met.parquet/met_", date, ".parquet"))
    message("Wrote: ", paste0(out_dir, "lecs_met.parquet/met_", date, ".parquet"), "\n")
    status |>
      arrow::arrow_table() |>
      #dplyr::group_by(year, month) |>
      arrow::write_dataset(paste0(out_dir, "lecs_status.parquet/status_", date, ".parquet"))
    message("Wrote: ", paste0(out_dir, "lecs_status.parquet/status_", date, ".parquet"), "\n")
    adv_data |>
      arrow::arrow_table() |>
      #dplyr::group_by(year, month) |>
      arrow::write_dataset(paste0(out_dir, "lecs_adv_data.parquet/adv_data_", date, ".parquet"))
    message("Wrote: ", paste0(out_dir, "lecs_adv_data.parquet/adv_data_", date, ".parquet"), "\n")
    message(tictoc::toc(), "\n")
  }
}

#' Process all LECS files
#'
#' Uses DuckDB to handle larger-than-memory data, and stores data in
#' Arrow/Parquet format.
#'
#' @param date A date to use for constructing output filenames and default file path
#' @param file_dir An optional directory to use instead of the default
#' @param files An optional list of files to process. Supersedes `date` and
#' `file_dir` for generating file list
#' @param out_dir An optional directory to use instead of the default
#' @param clean Set TRUE to remove bad data and timestamps
#'
#' @export
lecs_process_data_db <- function(date = NULL,
                                 file_dir = NULL,
                                 files = NULL,
                                 out_dir = "",
                                 clean = TRUE
                                ) {
  if ( is.null(c(date, file_dir, files)) ) {
    stop("Provide a date, file_dir, or a list of files to process.")
  }

  if (is.null(file_dir)) {
    file_dir <- paste0("data/SD Card Data/LECS_surface_sd/lecs_surface_", date)
  }

  if (is.null(files)) {
    files <- list.files(file_dir, pattern = "^202[3|4]", full.names = TRUE)
  }

  message(paste(length(files), "files to process"))

  # Process files into a list containing data frames for ADV, status, and Met

  future::plan(future::multicore)

  tictoc::tic("Time to read and process data: ")
  lecs_data <- lecs_parse_file(files[1])
  con <- lecs_create_db(lecs_data)
  lecs_data <- lecs_parse_files_db(con, files, clean)
  message(tictoc::toc())

  tictoc::tic("Time to write parquet: ")
  out_file <- paste0(out_dir, "lecs_met_", date, ".parquet")
  query <- paste0("COPY met TO '", out_file, "' (FORMAT PARQUET);")
  dbExecute(con, query)
  out_file <- paste0(out_dir, "lecs_status_", date, ".parquet")
  query <- paste0("COPY status TO '", out_file, "' (FORMAT PARQUET);")
  dbExecute(con, query)
  out_file <- paste0(out_dir, "lecs_adv_data", date, ".parquet")
  query <- paste0("COPY adv TO '", out_file, "' (FORMAT PARQUET);")
  dbExecute(con, query)
  DBI::dbDisconnect(con, shutdown = TRUE)
  message(tictoc::toc())
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
lecs_parse_files_p <- function(files, clean = FALSE) {
  furrr::future_map(files, lecs_parse_file, clean, .progress = TRUE) |>
    purrr::transpose() |>
    furrr::future_map(purrr::list_rbind)
}

#' Read LECS data from files and parse into duckdb
#'
#' @param files A list of file paths containing LECS data
#'
#' @return a list containing met data, status data, and ADV data
#' @export
lecs_parse_files_db <- function(con, files, clean = FALSE) {
  furrr::future_walk(files, lecs_parse_file_db, con = con, clean, .progress = TRUE)
}

#' Initialize a DuckDB for storing LECS data
#'
#' @param lecs_data
#'
#' @return a db connection
#' @export
lecs_create_db <- function(lecs_data) {
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = "duckdb"))
  DBI::dbCreateTable(con, "adv", lecs_data[["adv_data"]])
  DBI::dbCreateTable(con, "status", lecs_data[["status"]])
  DBI::dbCreateTable(con, "met", lecs_data[["met"]])
  con
}

#' Read LECS data from file and parse into DuckDB
#'
#' @param file A file path in LECS data format
#' @param clean Filter bad data and timestamps if true
#'
#' @return none
#' @export
lecs_parse_file_db <- function(con, file, clean = FALSE) {
  lecs_data <- lecs_parse_file(file, clean)
  DBI::dbAppendTable(con, "adv", lecs_data[["adv_data"]])
  DBI::dbAppendTable(con, "status", lecs_data[["status"]])
  DBI::dbAppendTable(con, "met", lecs_data[["met"]])
}

#' Read LECS data from file and parse into dataframes
#'
#' Runs file parsers, time alignment, and selects columns for output.
#'
#' @param file A file path in LECS data format
#' @param clean Filter bad data and timestamps if true
#'
#' @return a list containing LECS post_times, met data, status data, and ADV data
#' @export
#'
lecs_parse_file <- function(file, clean = FALSE) {
  df <- lecs_read_file(file)
  met <- lecs_met_data(df)
  status <- lecs_status_data(df)
  adv_data <- lecs_adv_data(df, rinko_cals)

  if (clean) {
    met <- lecs_clean_met(met)
    status <- lecs_clean_status(status)
    adv_data <- lecs_clean_adv_data(adv_data)
  }

  # Status timestamps fixed during cleaning
  # Should/can this be after cleaning data?
  # Needs to keep row/count info
  # remove garbage NA timestamps after

  adv_data <- adv_data |>
    make_lecs_ts(status) |>
    filter(!is.na(timestamp))

  # Select needed data here
  met <- met |>
    select(timestamp, PAR, wind_speed, wind_dir)

  status <- status |>
    select(timestamp, adv_timestamp,
           bat, soundspeed, heading, pitch, roll, temp)

  adv_data <- adv_data |>
    select(timestamp, pressure, u, v, w, amp1, amp2, amp3,
           corr1, corr2, corr3, temp, DO_percent, ph_counts, ox_umol_l, pH)

  # Where to fill missing timestamps and impute data?
  # Per file or for entire dataset?

  list(met = met,
       status = status,
       adv_data = adv_data)
}

