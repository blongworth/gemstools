library(tidyverse)
library(mlabtools)
library(testthat)

# RINKO_CALS
rinko_cals <- list(
  A = -1.219367e1,
  B = 2.134089e1,
  C = -3.559172e00,
  D = 6.691104e-01
)

df_raw <- read_lecs_web(start_date = "2024-01-25")
df <- lecs_add_metadata(df_raw) |>
  filter(send %in% 1:2)

post_times <- lecs_post_times(df)
met <- lecs_met_data(df)
status <- lecs_status_data(df)
adv_data <- lecs_adv_data(df, rinko_cals)

saveRDS(df, test_path("lecs_web.rds"))
saveRDS(status, test_path("lecs_status.rds"))
saveRDS(adv_data, test_path("lecs_data.rds"))

r4ds_test <- adv_data |>
  select(row_num, type, count) |>
  mutate(timestamp = as.POSIXct(NA)) |>
  dplyr::bind_rows(select(status, row_num, type, timestamp)) |>
  dplyr::arrange(row_num)

test_df <- test_ts |>
  select(row_num, type, count, timestamp) |>
  dplyr::bind_rows(select(status, row_num, type, timestamp)) |>
  dplyr::arrange(row_num)
saveRDS(r4ds_test, test_path("test_data_r4ds.rds"))

ts <-
