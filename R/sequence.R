#' Count missing elements in repeating 0-254 sequence
#'
#' @param numlist a vector of integers
#'
#' @return the number of missing elements
#' @export
count_missing <- function(numlist) {
  misscount = 0
  for (i in 2:length(numlist)) {
    if (numlist[i] == 0) {
      misscount = misscount + 255 - numlist[i-1]
    } else {
      misscount = misscount + numlist[i] - 1 - numlist[i-1]
    }
  }
  misscount
}

#' Calculate Fraction of missing elements
#'
#' @param numlist a vector of integers
#'
#' @return a fraction of missing elements
#' @export
fraction_missing <- function(numlist) {
  count_missing(numlist) / length(numlist)
}
