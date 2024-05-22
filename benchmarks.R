#speed testing time alignment

library(bench)

options(digits.secs=3)
test_status <- readRDS(test_path("lecs_status.rds"))
test_data <- readRDS(test_path("lecs_data.rds"))
prev_ts <- as.POSIXct("2024-01-24 16:11:11")
next_ts <- as.POSIXct("2024-01-24 16:13:29")

mark(counts_between(255,0))
mark(prev_status(test_status, 15))
mark(count_tzero(test_data, 14))
mark(calc_ts(241, prev_ts, next_ts, 241, 128))
mark(add_timestamp(test_data, test_status, 12))
mark(add_timestamps(test_data, test_status))
