test_that("Parsing lecs files works", {
  options(digits.secs=3)
  ls <- lecs_parse_file(test_path("test_file.txt"))
  expect_true(is.list(ls))
})

# test_that("Creating DuckDB works", {
#   lecs_data <- lecs_parse_file(test_path("test_file.txt"))
#   con <- lecs_create_db(lecs_data)
#   expect_s4_class(con, "duckdb_connection")
#   DBI::dbDisconnect(con, shutdown = TRUE)
#   file.remove(test_path(c("duckdb", "duckdb.wal")))
# })
#
# test_that("Populating DuckDB works", {
#   lecs_data <- lecs_parse_file(test_path("test_file.txt"))
#   con <- lecs_create_db(lecs_data)
#   lecs_parse_file_db(con, test_path("test_file.txt"))
#   count_adv <- DBI::dbGetQuery(con, "SELECT COUNT(*) as count_adv FROM adv") |>
#     dplyr::pull()
#   expect_equal(count_adv, 466)
#   lecs_parse_file_db(con, test_path("test_file.txt"))
#   count_adv <- DBI::dbGetQuery(con, "SELECT COUNT(*) as count_adv FROM adv") |>
#     dplyr::pull()
#   expect_equal(count_adv, 932)
#   DBI::dbDisconnect(con, shutdown = TRUE)
#   file.remove(test_path(c("duckdb", "duckdb.wal")))
# })
