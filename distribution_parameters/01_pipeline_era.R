# - SCRIPT TO OBTAIN DISTRIBUTION PARAMETERS OF BASELINE (1991-2020) ERA5 DATA
# - PARAMETERS OF TEMPERATURE AND PRECIP ARE USED TO BIAS-CORRECT NMME DATA
# - PARAMETERS OF WB (VARIABLE INTEGRATION WINDOWS) ARE USED TO CALCULATE WB ANOMALIES (PERCENTILES)

# SET UP ----------------------------------------------------------------------

library(tidyverse)
library(stars)
library(furrr)
library(lmom)

plan(multicore, workers = parallelly::availableCores() - 1)

# set data directory
source("distribution_parameters/setup.R")

# load general functions
source("functions/general_tools.R")
source("functions/tile.R")

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
# add 1 year at the beginning for rolling windows
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
s_tas[is.na(mask)] <- NA


# precipitation
s_precip <-
  load_data(
    dir_origin_cloud = "gs://clim_data_reg_useast1/era5/monthly_means/total_precipitation/",
    dir_dest_local = dir_data,
    date_vector = date_vector_full
  )

# mask land
s_precip[is.na(mask)] <- NA

# convert to mm/month

conv <-
  (units::set_units(1, month) / units::set_units(1, d)) |>
  units::drop_units()

s_precip <-
  s_precip |>
  units::drop_units() |>
  mutate(
    tp = tp * 1000 * conv,
    tp = units::set_units(tp, mm / month)
  )

## FIT DISTR TAS AND PRECIP ----

# temperature
distr_params(
  s_tas |> filter(year(time) >= 1991),
  dir_tmp_local = fs::path(dir_data, "tmp"),
  dir_output_cloud = "gs://clim_data_reg_useast1/era5/climatologies/",
  distribution = pelgno,
  f_name_root = "era5_2m-temperature_mon_norm-params_1991-2020",
  process = "era"
)


# precipitation
distr_params(
  s_precip |> filter(year(time) >= 1991),
  dir_tmp_local = fs::path(dir_data, "tmp"),
  dir_output_cloud = "gs://clim_data_reg_useast1/era5/climatologies_monthly_totals/",
  distribution = lmom::pelgam,
  f_name_root = "era5_total-precipitation_mon_gamma-params_1991-2020",
  process = "era"
)


## CALCULATE WATER BALANCE ----

# calculate PET
s_pet <-
  date_vector_full |>
  map(\(d) {
    pet_calculator_hamon(
      d,
      s_tas |>
        filter(time == d) |>
        adrop()
    )
  })

s_pet <-
  do.call(c, c(s_pet, along = "time")) |>
  st_set_dimensions("time", values = date_vector_full)

# calculate WB
s_pet <-
  s_pet |>
  units::drop_units() |>
  mutate(
    pet = pet * conv, # convert to mm/month
    pet = units::set_units(pet, mm / month)
  )

s_wb <-
  c(s_precip, s_pet) |>
  mutate(wb = tp - pet) |>
  select(wb)

## INTEGRATION WINDOWS ----

ss_wb_rolled <-
  c(3, 12) |>
  set_names() |>
  map(\(k_) {
    print(k_)
    rollsum(s_wb, k_, "wb_rollsum")
  })

## FIT DISTR WB ----

iwalk(ss_wb_rolled, \(s, k) {
  print(k)

  distr_params(
    s |> filter(year(time) >= 1991),
    dir_tmp_local = fs::path(dir_data, "tmp"),
    dir_output_cloud = "gs://clim_data_reg_useast1/era5/climatologies_monthly_totals/",
    distribution = pelglo,
    f_name_root = str_glue("era5_water-balance-hamon-rollsum{k}_mon_log-params_1991-2020"),
    process = "era",
    future_ = F
  )
})
