# - SCRIPT TO OBTAIN DISTRIBUTION PARAMETERS OF BASELINE (1991-2020) ERA5 DATA
# - PARAMETERS OF TEMPERATURE AND PRECIP ARE USED TO BIAS-CORRECT NMME DATA
# - PARAMETERS OF WB (VARIABLE INTEGRATION WINDOWS) ARE USED TO CALCULATE WB ANOMALIES (PERCENTILES)

# SET UP ----------------------------------------------------------------------

library(tidyverse)
library(stars)
library(mirai)
library(lmom)

daemons(parallel::detectCores() - 1)

# set data directory
source("distribution_parameters/setup.R")

# load general functions
source("functions/general_tools.R")

# load general drought functions
source("functions/functions_drought.R")

# load distribution functions
source("functions/functions_distributions.R")


# PROCESS ---------------------------------------------------------------------

# land mask
mask <-
  st_bbox(c(xmin = -0.125, ymin = -90.125, xmax = 359.875, ymax = 90.125), crs = 4326) |>
  st_as_stars(dx = 0.25) |>
  st_set_dimensions(c(1, 2), names = c("longitude", "latitude")) |>
  land_mask() |>
  suppressWarnings()


# dates to process
date_vector_full <-
  seq(as_date("1990-01-01"), as_date("2020-12-01"), by = "1 month")


# ******

## LOAD TAS AND PRECIP ----

# temperature
s_tas <-
  load_data(
    dir_origin_cloud = "gs://clim_data_reg_useast1/era5/monthly_means/2m_temperature/",
    dir_dest_local = dir_data,
    date_vector = date_vector_full
  )

# mask land
s_tas[mask == 0] <- NA


# precipitation
s_precip <-
  load_data(
    dir_origin_cloud = "gs://clim_data_reg_useast1/era5/monthly_means/total_precipitation/",
    dir_dest_local = dir_data,
    date_vector = date_vector_full
  )

# mask land
s_precip[mask == 0] <- NA

# convert to mm/month
s_precip <-
  s_precip |>
  mutate(
    tp = tp / units::set_units(1, d), # m -> mm -> x 30 days
    tp = units::set_units(tp, mm / month) |> round()
  )

## FIT DISTR TAS AND PRECIP ----

# temperature
distr_params(
  s_tas |> filter(year(time) >= 1991),
  dir_tmp_local = dir_data,
  dir_output_cloud = "gs://clim_data_reg_useast1/era5/climatologies/",
  distribution = pelgno,
  f_name_root = "era5_2m-temperature_mon_norm-params_1991-2020",
  process = "era"
)


# precipitation
distr_params(
  s_precip |> filter(year(time) >= 1991),
  dir_tmp_local = dir_data,
  dir_output_cloud = "gs://clim_data_reg_useast1/era5/climatologies_monthly_totals/",
  distribution = lmom::pelgam,
  f_name_root = "era5_total-precipitation_mon_gamma-params_1991-2020",
  process = "era"
)

# THIS SECTION NEEDS TO BE RECALCULATED WITH MONTHLY TOTAL PRECIP

# ## CALCULATE WATER BALANCE ----

# # heat index constants

# heat_vars <- heat_index_var_generator()

# # calculate water balance

# s_wb <-
#   date_vector_full |>
#   map(\(d) {
#     wb_calculator_th(
#       #
#       d,

#       s_tas |>
#         filter(time == d) |>
#         adrop() |>
#         setNames("tas") |>
#         mutate(tas = tas |> units::set_units(degC)),

#       s_precip |>
#         filter(time == d) |>
#         adrop() |>
#         setNames("pr"),

#       heat_vars
#     )
#   })

# s_wb <-
#   do.call(c, c(s_wb, along = "time")) |>
#   st_set_dimensions("time", values = date_vector_full)

# ## INTEGRATION WINDOWS ----

# ss_wb_rolled <-
#   c(3, 12) |>
#   set_names() |>
#   map(\(k_) {
#     print(k_)

#     rollsum(s_wb, k_, "wb_rollsum")
#   })

# ## FIT DISTR WB ----

# iwalk(ss_wb_rolled, \(s, k) {
#   print(k_)

#   distr_params(
#     s |> filter(year(time) >= 1991),
#     dir_tmp_local = "/mnt/pers_disk/tmp",
#     dir_output_cloud = "gs://clim_data_reg_useast1/era5/climatologies/",
#     distribution = pelglo,
#     f_name_root = str_glue("era5_water-balance-th-rollsum{k}_mon_log-params_1991-2020"),
#     process = "era"
#   )
# })
