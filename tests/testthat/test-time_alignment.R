# TODO: test how this handles future status dates
# TODO: test status timestamp correction

test_that("correct_status_timestamp_jitter removes future timestamps", {
  future_time <- Sys.time() + 3600
  past_time <- Sys.time() - 3600

  timestamps <- c(past_time, future_time)
  adv_timestamps <- c(past_time, future_time)

  result <- correct_status_timestamp_jitter(timestamps, adv_timestamps)

  expect_true(is.na(result[2]))
})

test_that("correct_status_timestamp_jitter works with empty vectors", {
  timestamps <- numeric(0)
  adv_timestamps <- numeric(0)

  result <- correct_status_timestamp_jitter(timestamps, adv_timestamps)

  expect_equal(length(result), 0)
})

test_that("correct_status_timestamp_jitter works with all future timestamps", {
  future_time <- Sys.time() + 3600
  past_time <- Sys.time() - 3600

  timestamps <- c(future_time, future_time)
  adv_timestamps <- c(past_time, past_time)

  result <- correct_status_timestamp_jitter(timestamps, adv_timestamps)

  expect_true(all(is.na(result)))
})

test_that("correct_status_timestamp_jitter works with all past timestamps", {
  past_time <- Sys.time() - 3600

  timestamps <- c(past_time, past_time)
  adv_timestamps <- c(past_time, past_time)

  result <- correct_status_timestamp_jitter(timestamps, adv_timestamps)

  expect_true(all(!is.na(result)))
  expect_equal(result, c(past_time, past_time))
})

test_that("correct_status_timestamp_jitter removes duplicate timestamps", {
  cur_time <- Sys.time()
  past_time1 <- cur_time - 1
  past_time2 <- cur_time - 2

  timestamps <- c(past_time2, past_time1, past_time1)
  adv_timestamps <- c(past_time2, past_time1, cur_time)

  result <- correct_status_timestamp_jitter(timestamps, adv_timestamps)

  expect_equal(length(result), 3)
  expect_equal(result, c(past_time2, past_time1, cur_time))
})

test_that("correct_status_timestamp_jitter removes gaps", {
  cur_time <- Sys.time()
  past_time1 <- cur_time - 1
  past_time2 <- cur_time - 2

  timestamps <- c(past_time2, cur_time, cur_time)
  adv_timestamps <- c(past_time2, past_time1, cur_time)

  result <- correct_status_timestamp_jitter(timestamps, adv_timestamps)

  expect_equal(length(result), 3)
  expect_equal(result, c(past_time2, past_time1, cur_time))
})
test_that("make_lecs_ts() works for 40 lines of data", {
  op <- options(digits.secs=3)
  on.exit(options(op), add = TRUE, after = FALSE)
  df <- readRDS(test_path("lecs_web.rds")) |>
    head(40)
  adv_ts_40 <- readRDS(test_path("adv_ts_40.rds"))
  status <- lecs_status_data(df)
  adv_data <- lecs_adv_data(df, rinko_cals)
  adv_data_ts <- make_lecs_ts(adv_data, status) |>
    select(row_num, count, timestamp)
  expect_false(any(duplicated(adv_data_ts$timestamp)))
  expect_identical(adv_data_ts, adv_ts_40)
})
