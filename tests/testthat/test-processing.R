test_that("Parsing lecs files works", {
  options(digits.secs=3)
  ls <- lecs_parse_file(test_path("test_file.txt"))
  expect_true(is.list(ls))
})
