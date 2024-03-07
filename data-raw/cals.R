# ph cals
# Cal from January SeapHOx cal
ph_cals <- list(
  int = 7.17,
  ph = 0.00011,
  temp = 0.00074
)

# RINKO_CALS
# G & H from 2024-03-05 lab cal, SN 0472
rinko_cals <- list(
  temp_A = -1.219367e1,
  temp_B = 2.134089e1,
  temp_C = -3.559172e00,
  temp_D = 6.691104e-01,
  o2_A = -4.382235e01,
  o2_B = 1.398755e02,
  o2_C = -4.119456e-01,
  o2_D = 9.934000e-03,
  o2_E = 4.000000e-03,
  o2_F = 4.440000e-05,
  o2_G = -3.5,
  o2_H = 0.936
)

usethis::use_data(ph_cals, overwrite = TRUE)
usethis::use_data(rinko_cals, overwrite = TRUE)
