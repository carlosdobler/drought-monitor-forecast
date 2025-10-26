# SCRIPT TO CALCULATE THE BASELINE (1991-2020) TAS
# AND PRECIP DISTRIBUTION PARAMETERS WITH NMME DATA

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
plan(multicore)


# load special functions
source("functions/general_tools.R")
# source("monitor_forecast/functions.R")

# load nmme models table
source("monitor_forecast/nmme_sources_df.R")


# temporary directory to save files
dir_tmp <- "/mnt/pers_disk/tmp"
if (fs::dir_exists(dir_tmp)) {
  fs::dir_delete(dir_tmp)
}
fs::dir_create(dir_tmp)


# root cloud path
dir_gs <- "gs://clim_data_reg_useast1/nmme"

# date vector
date_vector <-
  seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month")

vars <- c("precipitation", "average_temperature")


# loop through models
for (mod in df_sources$model) {
  print(str_glue("PROCESSING MODEL {mod}"))

  # download data (precip and tas)
  ff <-
    map(vars |> set_names(), \(var) {
      f <-
        rt_gs_list_files(str_glue("{dir_gs}/monthly/{mod}/{var}")) |>
        str_subset(str_flatten(
          seq(year(first(date_vector)), year(last(date_vector))),
          "|"
        ))

      f <-
        rt_gs_download_files(f, dir_tmp)
      # f <-
      #   str_glue("{dir_tmp}/{fs::path_file(f)}")

      return(f)
    })

  # loop through months
  str_pad(seq(12), 2, "left", "0") |>
    walk(\(mon) {
      print(str_glue("   PROCESSING MONTH {mon}"))

      # subset 1 calendar month files
      # (2 variables; 30 years)
      ff_m <-
        ff |>
        map(\(f) str_subset(f, str_glue("-{mon}-")))

      ff_m |>
        # for each variable
        iwalk(\(f, i) {
          # load data
          ss <-
            f |>
            future_map(read_ncdf, proxy = F) |>
            suppressMessages()

          # pool all members from all years
          ss <-
            do.call(c, c(ss, along = "M"))

          # convert precip units: mm -> m (same as ERA5)
          if (i == "precipitation") {
            ss <-
              ss |>
              mutate(prec = prec |> units::set_units(m / d))
          }

          # for each lead step
          r <-
            seq(dim(ss)["L"]) |>
            map(\(lead_in) {
              print(str_glue(
                "      PROCESSING VARIABLE {i}  |  LEAD {lead_in}"
              ))

              s <-
                ss |>
                # subset lead step
                slice(L, lead_in) |>
                units::drop_units() |>

                # fit distributions
                st_apply(
                  c(1, 2),
                  \(x) {
                    # normal dist for temperature
                    if (i == "average_temperature") {
                      if (any(is.na(x))) {
                        params <- c(xi = NA, alpha = NA, k = NA)
                      } else {
                        lmoms <- lmom::samlmu(x)
                        params <- lmom::pelgno(lmoms)
                      }

                      # logistic dist for precipitation
                    } else if (i == "precipitation") {
                      if (any(is.na(x))) {
                        params <- c(alpha = NA, beta = NA)
                      } else if (all(x == 0)) {
                        params <- c(alpha = -9999, beta = -9999)
                      } else {
                        x[x == 0] <- 1e-5 # avoid errors when fitting
                        lmoms <- lmom::samlmu(x)
                        params <- lmom::pelgam(lmoms)
                      }
                    }

                    return(params)
                  },
                  FUTURE = T,
                  .fname = "params"
                ) |>
                split("params")

              return(s)
            })

          r <-
            do.call(c, c(r, along = "L"))

          # result file name
          f <-
            case_when(
              i == "average_temperature" ~
                str_glue(
                  "{dir_tmp}/nmme_{mod}_average-temperature_mon_norm-params_1991-2020_{mon}_leads-6.nc"
                ),
              i == "precipitation" ~
                str_glue(
                  "{dir_tmp}/nmme_{mod}_precipitation_mon_gamma-params_1991-2020_{mon}_leads-6.nc"
                )
            )

          # write to disk
          rt_write_nc(r, f)

          # move to bucket
          str_glue(
            "gcloud storage mv {f} gs://clim_data_reg_useast1/nmme/climatologies/{mod}/"
          ) |>
            system(ignore.stdout = T, ignore.stderr = T)
        })
    })

  # clean up
  walk(ff, \(f) future_walk(f, fs::file_delete))
}


# clean up
fs::dir_delete(dir_tmp)
