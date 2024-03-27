test_that("zero velocity gives zero flux", {
  w = rep(0, 28801)
  val = rnorm(28801, mean = 10)
  time = seq(as.POSIXct("2024-01-01 00:00:00"), as.POSIXct("2024-01-01 01:00:00"), by = 1/8)
  expect_equal(calc_flux(val, w, time, frequency = 1/8), 0)
})

test_that("positive velocity gives positive flux", {
  w = rep(1, 28801)
  val = rnorm(28801, mean = 10)
  time = seq(as.POSIXct("2024-01-01 00:00:00"), as.POSIXct("2024-01-01 01:00:00"), by = 1/8)
  expect_true(calc_flux(val, w, time, frequency = 1/8) > 0)
})


test_that("zero val gives zero flux", {
  w = rnorm(28801, mean = 0)
  val = rep(0, 28801)
  time = seq(as.POSIXct("2024-01-01 00:00:00"), as.POSIXct("2024-01-01 01:00:00"), by = 1/8)
  expect_equal(calc_flux(val, w, time, frequency = 1/8), 0)
})
