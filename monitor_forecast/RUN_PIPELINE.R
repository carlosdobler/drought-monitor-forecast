library(tidyverse)
library(stars)
library(furrr)

plan(multicore, workers = parallelly::availableCores() - 1)

# get date
args <- commandArgs(trailingOnly = TRUE)
date_to_proc <- str_glue("{args[1]}-{args[2]}-01")

# load functions and setup params
source("functions/functions_drought.R")
source("functions/general_tools.R")
source("monitor_forecast/setup.R")

# key bucket directories
dir_gs_era <- "gs://clim_data_reg_useast1/era5"
dir_gs_nmme <- "gs://clim_data_reg_useast1/nmme"

# conversion constant (days in month)
conv <-
  (units::set_units(1, month) / units::set_units(1, d)) |>
  units::drop_units()

# integration windows
winds <- c(12, 3)

# run monitor script
source("monitor_forecast/1_era_monitor_generator.R")

# run forecast script
source("monitor_forecast/2_nmme_forecast_generator.R")

plan(sequential)
