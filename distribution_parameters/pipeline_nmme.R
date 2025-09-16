
# SET UP ----------------------------------------------------------------------

library(tidyverse)
library(stars)
library(furrr)
library(lmom)

# parallel config
options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
plan(multicore)

# load general functions
source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

# load general drought functions
source("misc/functions_drought.R")

# load project functions
source("distribution_parameters/functions.R")


# PROCESS ---------------------------------------------------------------------

# load nmme models table
source("monitor_forecast/nmme_sources_df.R")

# land mask
mask <- 
  st_bbox(c(xmin = -0.5,
            ymin = -90.5,
            xmax = 359.5,
            ymax = 90.5),
          crs = 4326) |> 
  st_as_stars(dx = 1) |> 
  st_set_dimensions(c(1,2), names = c("X", "Y")) |> 
  land_mask() |> 
  suppressWarnings()


# dates to process
date_vector_full <- 
  seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month")
# some nmme models don't include 1990



mod = df_sources$model[1]

## LOAD TAS AND PRECIP ----

# temperature
s_tas <- 
  load_data(dir_origin_cloud = str_glue("gs://clim_data_reg_useast1/nmme/monthly/{mod}/average_temperature/"),
            dir_dest_local = "/mnt/pers_disk/tmp/",
            date_vector = date_vector_full)

# mask land
s_tas[mask == 0] <- NA


# # precipitation
# s_precip <- 
#   load_data(dir_origin_cloud = "gs://clim_data_reg_useast1/era5/monthly_means/total_precipitation/",
#             dir_dest_local = "/mnt/pers_disk/tmp/",
#             date_vector = date_vector_full)
# 
# # mask land
# s_precip[mask == 0] <- NA
# 
# # remove zeros to avoid errors when fitting gamma
# s_precip[s_precip == 0] <- 1e-10



distr_params(s_tas,
             )






