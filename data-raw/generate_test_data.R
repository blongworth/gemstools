library(tidyverse)
library(mlabtools)
library(testthat)
library(arrow)

# lecs data
df_raw <- lecs_read_web(start_date = "2024-01-25")
df <- lecs_add_metadata(df_raw) |>
  filter(send %in% 1:2)

post_times <- lecs_post_times(df)
met <- lecs_met_data(df)
status <- lecs_status_data(df)
adv_data <- lecs_adv_data(df, rinko_cals)

saveRDS(df, test_path("lecs_web.rds"))
saveRDS(status, test_path("lecs_status.rds"))
saveRDS(adv_data, test_path("lecs_data.rds"))

# parquet data
sp <- read_seaphox(test_path("seaphox_test.csv"))
adv <- readRDS(test_path("adv_df_dec.rds"))
write_parquet(adv, test_path("adv.parquet"))

# Shift time of manually timestamped adv data
adv_ts_40 <- readRDS(test_path("adv_ts_40_EST.rds")) |>
  mutate(timestamp = timestamp + 3600 * 4) # Adjust to match TZ=UTC

saveRDS(adv_ts_40, test_path("adv_ts_40.rds"))

# test data for duplicate timestamp issue
options(digits.secs=3)
df <- lecs_parse_file("data-raw/2023_08_05_12_12_17.txt", clean = TRUE)

df$adv_data |>
  group_by(timestamp) |>
  summarize(n = n()) |>
  filter(n > 1)

df$adv_data |>
  filter(timestamp > "2023-08-05 08:17:18",
         timestamp < "2023-08-05 08:17:30") |>
  view()

# test data for r4ds/SO time alignment q
# no longer needed

#r4ds_test <- adv_data |>
#  select(row_num, type, count) |>
#  mutate(timestamp = as.POSIXct(NA)) |>
#  dplyr::bind_rows(select(status, row_num, type, timestamp)) |>
#  dplyr::arrange(row_num)

#test_df <- test_ts |>
#  select(row_num, type, count, timestamp) |>
#  dplyr::bind_rows(select(status, row_num, type, timestamp)) |>
#  dplyr::arrange(row_num)
#saveRDS(r4ds_test, test_path("test_data_r4ds.rds"))

# need to add generators for test_file.txt, adv_ts_21.rds, and adv_ts_40.rds

# seaphox data is part of a seaphox file

