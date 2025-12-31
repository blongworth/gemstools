sp <- read_seaphox(test_path("seaphox_test.csv"))
adv <- readRDS(test_path("adv_df_dec.rds"))

test_that("generate_ph_model returns fit parameters", {
  expect_type(generate_ph_model(sp, adv), "double")
})

test_that("generate_ph_model works with arrow datasets", {
  adv <- arrow::open_dataset(test_path("adv.parquet"))
  expect_type(generate_ph_model(sp, adv), "double")
})

test_that("cal_ph returns fitted ph values", {
  ph_fit <- generate_ph_model(sp, adv)
  ph <- cal_ph(adv$ph_counts, adv$temp, ph_fit)
  expect_type(ph, "double")
  expect_true(mean(ph) > 8 & mean(ph) < 8.2)
})

# won't work until adv.parquet is updated to include ox_umol_l
# test_that("generate_o2_model works with arrow datasets", {
#   adv <- arrow::open_dataset(test_path("adv.parquet"))
#   expect_type(generate_o2_model(sp, adv), "double")
# })

test_that("read_seaphox returns seaphox data", {
  sp <- read_seaphox(test_path("seaphox_test.csv"))
  expect_s3_class(sp, "data.frame")
})

test_that("co2 conversion returns finite positive values", {
  res <- co2_ppm_to_umol_per_l(xco2_ppm = 415, temp_c = 20, sal_psu = 35)
  expect_type(res, "double")
  expect_true(is.finite(res))
  expect_gt(res, 0)
})

test_that("co2 vectorization and recycling work across inputs", {
  x <- c(400, 420, 450)
  t <- 20
  s <- c(34, 35, 36)
  p <- 1013.25
  out <- co2_ppm_to_umol_per_l(x, t, s, p)
  expect_length(out, 3)
  expect_true(all(is.finite(out)))
})

test_that("co2 freshwater (S=0) yields higher solubility than seawater at same conditions", {
  x <- 415
  t <- 10
  p <- 1013.25
  seawater <- co2_ppm_to_umol_per_l(x, t, sal_psu = 35, pressure_mbar = p)
  freshwater <- co2_ppm_to_umol_per_l(x, t, sal_psu = 0, pressure_mbar = p)
  expect_gt(freshwater, seawater)
})

test_that("higher temperature yields lower dissolved CO2 (solubility decreases with T)", {
  x <- 415
  s <- 35
  p <- 1013.25
  cold <- co2_ppm_to_umol_per_l(x, temp_c = 5, sal_psu = s, pressure_mbar = p)
  warm <- co2_ppm_to_umol_per_l(x, temp_c = 25, sal_psu = s, pressure_mbar = p)
  expect_gt(cold, warm)
})

test_that("co2 pressure scaling behaves linearly", {
  x <- 415
  t <- 20
  s <- 35
  res_1atm <- co2_ppm_to_umol_per_l(x, t, s, pressure_mbar = 1013.25)
  res_0_8atm <- co2_ppm_to_umol_per_l(x, t, s, pressure_mbar = 0.8 * 1013.25)
  # Expect ~20% lower at 0.8 atm
  expect_lt(abs(res_0_8atm / res_1atm - 0.8), 0.02) # within 2%
})

test_that("co2 density parameter affects mol/kg -> mol/L conversion", {
  x <- 415
  t <- 20
  s <- 35
  base <- co2_ppm_to_umol_per_l(x, t, s, density_kg_per_l = 1.0)
  denser <- co2_ppm_to_umol_per_l(x, t, s, density_kg_per_l = 1.025)
  expect_gt(denser, base)
  expect_lt(abs(denser / base - 1.025), 0.01) # within 1% tolerance
})

test_that("co2 input validation errors are thrown for invalid inputs", {
  expect_error(co2_ppm_to_umol_per_l(-1, 20, 35), "xco2_ppm")
  expect_error(co2_ppm_to_umol_per_l(415, NA, 35), "temp_c")
  expect_error(co2_ppm_to_umol_per_l(415, 20, -1), "sal_psu")
  expect_error(co2_ppm_to_umol_per_l(415, 20, 35, pressure_mbar = 0), "pressure_mbar")
  expect_error(co2_ppm_to_umol_per_l(415, 20, 35, density_kg_per_l = 0), "density_kg_per_l")
})

test_that("co2 handles vector inputs for all parameters with expected length", {
  res <- co2_ppm_to_umol_per_l(
    xco2_ppm = c(400, 410),
    temp_c = c(10, 20, 30),
    sal_psu = c(0, 35),
    pressure_mbar = c(1000, 1013.25, 1020, 990)
  )
  expect_length(res, max(2, 3, 2, 4)) # 4
  expect_true(all(is.finite(res)))
})
