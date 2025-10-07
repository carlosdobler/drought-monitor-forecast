library(tidyverse)
library(stars)
library(mirai)

daemons(parallel::detectCores() - 1)

# load special functions
source("functions/drought.R")
source("functions/general_tools.R")

# load script params:
# date to process and temporary data directory
source("monitor_forecast/pipeline_params.R")
fs::dir_create(dir_data)

# key bucket directories
dir_gs_era <- "gs://clim_data_reg_useast1/era5"
dir_gs_nmme <- "gs://clim_data_reg_useast1/nmme/climatologies"

# run monitor script
source("monitor_forecast/1_era_monitor_generator.R")

# run forecast script
source("monitor_forecast/2_nmme_forecast_generator.R")

# delete temporary data directory
fs::dir_delete(dir_data)
