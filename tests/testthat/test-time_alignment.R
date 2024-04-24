# TODO: test how this handles future status dates
# TODO: test status timestamp correction

test_that("make_lecs_ts() works for 21 lines of data", {
  op <- options(digits.secs=3)
  on.exit(options(op), add = TRUE, after = FALSE)
  df <- readRDS(test_path("lecs_web.rds")) |>
    head(21)
  adv_ts_21 <- readRDS(test_path("adv_ts_21.rds"))
  status <- lecs_status_data(df)
  adv_data <- lecs_adv_data(df, rinko_cals)
  adv_data_ts <- make_lecs_ts(adv_data, status) |>
    select(row_num, count, timestamp)
  expect_identical(adv_data_ts, adv_ts_21)
})

test_that("make_lecs_ts() adds time components", {
  df <- readRDS(test_path("lecs_web.rds")) |>
    head(21)
  status <- lecs_status_data(df)
  adv_data <- lecs_adv_data(df, rinko_cals)
  adv_data_ts <- make_lecs_ts(adv_data, status)
  expect_identical(adv_data_ts[['year']][1], 2024)
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
