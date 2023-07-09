test_that("no missing data detected correctly", {
  expect_equal(count_missing(0:254), 0)
})
