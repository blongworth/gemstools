# Functions for processing raw gems data

#' Process all GEMS files in a dated folder
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
#' @return A character vector of output file paths (invisible)
#'
#' @export
gems_process_data <- function(
  date = NULL,
  file_dir = NULL,
  files = NULL,
  out_dir = ".",
  clean = TRUE,
  dedupe = FALSE,
  resample = FALSE,
  csv = FALSE,
  parquet = TRUE
) {
  if (is.null(c(date, file_dir, files))) {
    stop("Provide a date, file_dir, or a list of files to process.")
  }

  if (is.null(file_dir)) {
    file_dir <- paste0("data/SD Card Data/gems_surface_sd/gems_surface_", date)
  }

  if (is.null(files)) {
    files <- list.files(file_dir, pattern = "^gems_202[5]", full.names = TRUE)
  }

  if (!length(files)) {
    stop("No files found to process.")
  }

  message(paste(length(files), "files to process"))

  # Process files into a list containing data frames for ADV, status, and RGA

  future::plan(future::multicore)

  tictoc::tic("Time to read and process data: ")
  gems_data <- gems_parse_files(files, clean)
  message(tictoc::toc(), "\n")

  attach(gems_data)

  if (dedupe) {
    adv_data <- adv_data |>
      dplyr::distinct(timestamp, .keep_all = TRUE)
  }

  if (resample) {
    adv_data <- adv_data |>
      dplyr::group_by(clock::date_group(timestamp, resample)) |>
      dplyr::summarise(dplyr::across(
        dplyr::everything(),
        ~ mean(.x, na.rm = TRUE)
      ))
  }

  output_paths <- character()

  if (csv) {
    tictoc::tic("Time to write csvs: ")
    csv_rga <- paste0(out_dir, "gems_rga", date, ".csv")
    csv_status <- paste0(out_dir, "gems_status_", date, ".csv")
    csv_adv <- paste0(out_dir, "gems_adv_data_", date, ".csv")
    data.table::fwrite(rga, csv_rga)
    data.table::fwrite(status, csv_status)
    data.table::fwrite(adv_data, csv_adv)
    output_paths <- c(output_paths, csv_rga, csv_status, csv_adv)
    message(tictoc::toc(), "\n")
  }

  if (parquet) {
    tictoc::tic("Time to write parquet: ")
    if (is.null(date)) {
      path_rga <- file.path(out_dir, "gems_rga.parquet")
      path_status <- file.path(out_dir, "gems_status.parquet")
      path_adv <- file.path(out_dir, "gems_adv.parquet")
      rga |>
        arrow::arrow_table() |>
        arrow::write_dataset(path_rga)
      message("Wrote: ", path_rga, "\n")
      status |>
        arrow::arrow_table() |>
        arrow::write_dataset(path_status)
      message("Wrote: ", path_status, "\n")
      adv_data |>
        arrow::arrow_table() |>
        arrow::write_dataset(path_adv)
      message("Wrote: ", path_adv, "\n")
      output_paths <- c(output_paths, path_rga, path_status, path_adv)
    } else {
      dir.create(file.path(out_dir, "gems_rga"), showWarnings = FALSE)
      dir.create(
        file.path(out_dir, "gems_status.parquet"),
        showWarnings = FALSE
      )
      dir.create(
        file.path(out_dir, "gems_adv_data.parquet"),
        showWarnings = FALSE
      )
      path_rga <- paste0(out_dir, "gems_rga/rga", date, ".parquet")
      path_status <- paste0(
        out_dir,
        "gems_status.parquet/status_",
        date,
        ".parquet"
      )
      path_adv <- paste0(
        out_dir,
        "gems_adv_data.parquet/adv_data_",
        date,
        ".parquet"
      )
      rga |>
        arrow::arrow_table() |>
        arrow::write_dataset(path_rga)
      message("Wrote: ", path_rga, "\n")
      status |>
        arrow::arrow_table() |>
        arrow::write_dataset(path_status)
      message("Wrote: ", path_status, "\n")
      adv_data |>
        arrow::arrow_table() |>
        arrow::write_dataset(path_adv)
      message("Wrote: ", path_adv, "\n")
      output_paths <- c(output_paths, path_rga, path_status, path_adv)
    }
    message(tictoc::toc(), "\n")
  }

  invisible(output_paths)
}

#' Parallelized Read gems data from files and parse into dataframes
#'
#' @param files A list of file paths containing gems data
#'
#' @return a list containing rga data, status data, and ADV data
#' @export
gems_parse_files <- function(files, clean = FALSE) {
  if (!length(files)) {
    stop("No files provided to parse.")
  }
  furrr::future_map(files, gems_parse_file, clean, .progress = TRUE) |>
    purrr::transpose() |>
    furrr::future_map(purrr::list_rbind)
}

#' Read gems data from file and parse into dataframes
#'
#' Runs file parsers, time alignment, and selects columns for output.
#'
#' @param file A file path in gems data format
#' @param clean Filter bad data and timestamps if true
#'
#' @return a list containing gems post_times, rga data, status data, and ADV data
#' @export
#'
gems_parse_file <- function(file, clean = FALSE) {
  # handle empty files
  if (file.info(file)$size == 0) {
    warning(paste("File", file, "is empty. Skipping."))
    return(list(
      rga = data.frame(),
      status = data.frame(),
      adv_data = data.frame()
    ))
  }
  df <- gems_read_file(file)
  rga <- gems_rga_data(df)
  status <- gems_status_data(df)
  adv_data <- gems_adv_data(df)

  # Check if any data type is missing
  if (nrow(rga) == 0) {
    warning(paste("File", file, "contains no RGA data"))
  }
  if (nrow(status) == 0) {
    warning(paste("File", file, "contains no status data"))
  }
  if (nrow(adv_data) == 0) {
    warning(paste("File", file, "contains no ADV data"))
  }

  if (clean) {
    if (nrow(rga) > 0) {
      rga <- gems_clean_rga(rga)
    }
    if (nrow(status) > 0) {
      status <- gems_clean_status(status)
    }
    if (nrow(adv_data) > 0) adv_data <- gems_clean_adv_data(adv_data)
  }

  # Status timestamps fixed during cleaning
  # Should/can this be after cleaning data?
  # Needs to keep row/count info
  # remove garbage NA timestamps after

  # Only process ADV timestamps if both status and adv_data exist
  # make adv_timestamp_cor the main timestamp
  # to deal with GEMS serial delay issue
  if (nrow(adv_data) > 0 && nrow(status) > 0) {
    status <- status |>
      dplyr::mutate(timestamp = adv_timestamp_cor)
    adv_data <- adv_data |>
      make_gems_ts(status) |>
      filter(!is.na(timestamp))
  }

  # Select needed data here - only if columns exist
  if (nrow(rga) > 0) {
    rga <- rga |>
      select(timestamp, mass, current, pressure)
  }
  if (nrow(status) > 0) {
    status <- status |>
      select(
        timestamp,
        adv_timestamp,
        bat,
        soundspeed,
        heading,
        pitch,
        roll,
        temp
      )
  }

  if (nrow(adv_data) > 0) {
    adv_data <- adv_data |>
      select(
        timestamp,
        pressure,
        u,
        v,
        w,
        amp1,
        amp2,
        amp3,
        corr1,
        corr2,
        corr3
      )
  }

  if (clean && nrow(adv_data) > 0) {
    # select only the first row when there are duplicate timestamps
    adv_data <- adv_data |>
      dplyr::group_by(timestamp) |>
      dplyr::slice(1) |>
      dplyr::ungroup()
  }
  # Where to fill missing timestamps and impute data?
  # Per file or for entire dataset?

  list(
    rga = rga,
    status = status,
    adv_data = adv_data
  )
}
