library(tidyverse)
library(mlabtools)
library(testthat)

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

