test_that("counts_between works", {
  expect_equal(counts_between(1, 1), 0)
  expect_equal(counts_between(0, 1), 1)
  expect_equal(counts_between(255, 0), 1)
  expect_equal(counts_between(NA, 0), NA)
})

test_that("getting status lines works", {
  test_status <- readRDS(test_path("lecs_status.rds"))
  expect_equal(prev_status(test_status, 15), 14)
  expect_equal(prev_status(test_status, 21), 20)
  expect_equal(prev_status(test_status, 13), NA)
  expect_equal(next_status(test_status, 13), 14)
  expect_equal(next_status(test_status, 19), 20)
  expect_equal(next_status(test_status, 1001), NA)
})

test_that("finding count at t0 works", {
  test_data <- readRDS(test_path("lecs_data.rds"))
  expect_equal(count_tzero(test_data, 14), 241)
  expect_equal(count_tzero(test_data, 1002), NA)
})

test_that("calc_ts() works", {
  options(digits.secs=3)
  test_status <- readRDS(test_path("lecs_status.rds"))
  test_data <- readRDS(test_path("lecs_data.rds"))
  prev_ts <- as.POSIXct("2024-01-24 16:11:11",
                        tz = "America/New_York")
  next_ts <- as.POSIXct("2024-01-24 16:13:29",
                        tz = "America/New_York")
  expect_equal(calc_ts(241, prev_ts, next_ts, 241, 128),
               as.POSIXct("2024-01-24 16:11:11.000",
                          tz = "America/New_York"))
  expect_equal(calc_ts(243, prev_ts, next_ts, 241, 128),
               as.POSIXct("2024-01-24 16:11:11.000",
                          tz = "America/New_York") + 1/16 * 2)
  expect_equal(calc_ts(126, prev_ts, next_ts, 241, 128),
               as.POSIXct("2024-01-24 16:13:28.000",
                          tz = "America/New_York") - 1/16 * 2)
})

test_that("add_timestamp() works", {
  options(digits.secs=3)
  test_status <- readRDS(test_path("lecs_status.rds"))
  test_data <- readRDS(test_path("lecs_data.rds"))
  expect_equal(add_timestamp(test_data, test_status, 12),
               as.POSIXct("2024-01-24 16:11:11.000",
                          tz = "America/New_York"))
  expect_equal(add_timestamp(test_data, test_status, 14),
               as.POSIXct("2024-01-24 16:11:11.000",
                          tz = "America/New_York") + 1/16 * 2)
  expect_equal(add_timestamp(test_data, test_status, 15),
               as.POSIXct("2024-01-24 16:13:29.000",
                          tz = "America/New_York") - 1/16 * 2)
  expect_equal(add_timestamp(test_data, test_status, 466),
               as.POSIXct("2024-01-24 16:13:59.000",
                          tz = "America/New_York") - 1/16 * 3)
  expect_equal(add_timestamp(test_data, test_status, 467),
               as.POSIXct("2024-01-24 20:11:12.000",
                          tz = "America/New_York") - 1/16 * 10)
})

#test_that("make_lecs_ts() works for 21 lines of data", {
#  options(digits.secs=3)
#  df <- readRDS(test_path("lecs_web.rds")) |>
#    head(21)
#  adv_ts_21 <- readRDS(test_path("adv_ts_21.rds"))
#  status <- lecs_status_data(df)
#  adv_data <- lecs_adv_data(df, rinko_cals)
#  adv_data_ts <- make_lecs_ts(adv_data, status) |>
#    select(row_num, count, timestamp)
#  expect_identical(adv_data_ts, adv_ts_21)
#})

#test_that("make_lecs_ts() works for 40 lines of data", {
#  options(digits.secs=3)
#  df <- readRDS(test_path("lecs_web.rds")) |>
#    head(40)
#  adv_ts_40 <- readRDS(test_path("adv_ts_40.rds"))
#  status <- lecs_status_data(df)
#  adv_data <- lecs_adv_data(df, rinko_cals)
#  adv_data_ts <- make_lecs_ts(adv_data, status) |>
#    select(row_num, count, timestamp)
#  expect_identical(adv_data_ts, adv_ts_40)
#})

test_that("make_lecs_ts_2() works for 21 lines of data", {
  options(digits.secs=3)
  df <- readRDS(test_path("lecs_web.rds")) |>
    head(21)
  adv_ts_21 <- readRDS(test_path("adv_ts_21.rds"))
  status <- lecs_status_data(df)
  adv_data <- lecs_adv_data(df, rinko_cals)
  adv_data_ts <- make_lecs_ts_2(adv_data, status) |>
    select(row_num, count, timestamp)
  expect_identical(adv_data_ts, adv_ts_21)
})

test_that("make_lecs_ts_2() works for 40 lines of data", {
  options(digits.secs=3)
  df <- readRDS(test_path("lecs_web.rds")) |>
    head(40)
  adv_ts_40 <- readRDS(test_path("adv_ts_40.rds"))
  status <- lecs_status_data(df)
  adv_data <- lecs_adv_data(df, rinko_cals)
  adv_data_ts <- make_lecs_ts_2(adv_data, status) |>
    select(row_num, count, timestamp)
  expect_false(any(duplicated(adv_data_ts$timestamp)))
  expect_identical(adv_data_ts, adv_ts_40)
})
