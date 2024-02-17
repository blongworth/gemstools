test_that("generate_ph_model returns fit parameters", {
  sp <- read_seaphox(test_path("seaphox_test.csv"))
  adv <- readRDS(test_path("adv_df_dec.rds"))
  expect_type(generate_ph_model(sp, adv), "double")
})

test_that("fit_ph_model returns fitted ph values", {
  sp <- read_seaphox(test_path("seaphox_test.csv"))
  adv <- readRDS(test_path("adv_df_dec.rds"))
  ph_fit <- generate_ph_model(sp, adv)
  ph <- fit_ph_model(adv$ph_counts, adv$temp, ph_fit)
  expect_type(ph, "double")
  expect_true(mean(ph) > 8 & mean(ph) < 8.2)
})

test_that("read_seaphox returns seaphox data", {
  sp <- read_seaphox(test_path("seaphox_test.csv"))
  expect_s3_class(sp, "data.frame")
})
