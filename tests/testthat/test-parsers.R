test_that("Parsing lecs files works", {
  options(digits.secs=3)
  ls <- lecs_parse_file(test_path("test_file.txt"))
  expect_true(is.list(ls))
})

test_that("File read works", {
  df <- lecs_read_file(test_path("test_file.txt"))
  expect_true(is.data.frame(df))
})

test_that("Parsing Status works", {
  df <- lecs_read_file(test_path("test_file.txt"))
  status <- lecs_status_data(df)
  expect_true(is.data.frame(status))
})

test_that("Parsing Data works", {
  df <- lecs_read_file(test_path("test_file.txt"))
  adv <- lecs_adv_data(df, rinko_cals)
  expect_true(is.data.frame(adv))
})

test_that("Missing rows calculated correctly for files", {
  df <- lecs_read_file(test_path("test_file.txt"))
  adv <- lecs_adv_data(df, rinko_cals)
  missing <- lecs_missing(adv$count)
  expect_true(is.integer(missing))
  expect_true(is.na(missing[1]))
})

test_that("Missing rows calculated correctly for web sends", {
  adv <- readRDS(test_path("lecs_data.rds"))
  missing <- lecs_missing(adv$count, adv$line)
  expect_true(is.integer(missing))
  expect_true(is.na(missing[1]))
  expect_true(is.na(missing[467]))
})
