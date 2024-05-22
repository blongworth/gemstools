test_that("File read works", {
  df <- lecs_read_file(test_path("test_file.txt"))
  expect_true(is.data.frame(df))
  expect_equal(nrow(df), 500)
})

test_that("Parsing Status works", {
  df <- lecs_read_file(test_path("test_file.txt"))
  status <- lecs_status_data(df)
  expect_true(is.data.frame(status))
  expect_equal(nrow(status), 32)
})

test_that("Cleaning Status works", {
  df <- lecs_read_file(test_path("test_file.txt"))
  status <- lecs_status_data(df)
  status_clean <- lecs_clean_status(status)
  expect_true(is.data.frame(status_clean))
  expect_equal(nrow(status_clean), 10)
})

test_that("Parsing Met works", {
  df <- lecs_read_file(test_path("test_file.txt"))
  met <- lecs_met_data(df)
  expect_true(is.data.frame(met))
  expect_equal(nrow(met), 2)
})

test_that("Parsing Data works", {
  df <- lecs_read_file(test_path("test_file.txt"))
  adv <- lecs_adv_data(df, rinko_cals)
  expect_true(is.data.frame(adv))
})

test_that("Parsing Data doesn't skip rows", {
  df <- lecs_read_file(test_path("test_file.txt"))
  adv <- lecs_adv_data(df, rinko_cals)
  expect_equal(nrow(adv), 466)
})

test_that("Cleaning ADV Data works", {
  df <- lecs_read_file(test_path("test_file.txt"))
  status <- lecs_status_data(df)
  adv <- lecs_adv_data(df, rinko_cals) |>
    make_lecs_ts(status)
  adv_clean <- lecs_clean_adv_data(adv)
  expect_true(is.data.frame(adv_clean))
  expect_equal(nrow(adv_clean), 466)
})

test_that("Missing rows calculated correctly for files", {
  df <- lecs_read_file(test_path("test_file.txt"))
  adv <- lecs_adv_data(df, rinko_cals)
  missing <- lecs_missing(adv$count)
  expect_true(is.integer(missing))
  expect_true(is.na(missing[1]))
  expect_equal(sum(missing, na.rm = TRUE), 165)
})

test_that("Missing rows calculated correctly for web sends", {
  adv <- readRDS(test_path("lecs_data.rds"))
  missing <- lecs_missing(adv$count, adv$line)
  expect_true(is.integer(missing))
  expect_true(is.na(missing[1]))
  expect_true(is.na(missing[467]))
})
