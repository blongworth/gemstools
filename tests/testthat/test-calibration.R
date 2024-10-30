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
