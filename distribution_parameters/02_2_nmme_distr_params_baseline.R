# SCRIPT TO CALCULATE THE BASELINE (1991-2020) TAS
# AND PRECIP DISTRIBUTION PARAMETERS WITH NMME DATA

library(tidyverse)
library(stars)
library(mirai)
library(lmom)

daemons(parallel::detectCores() - 1)
cl <- make_cluster(parallel::detectCores() - 1)

# set data directory
source("distribution_parameters/setup.R")

# load special functions
source("functions/general_tools.R")
source("functions/functions_distributions.R")

# load nmme models table
source("monitor_forecast/nmme_sources_df.R")


# temporary directory to save files
if (fs::dir_exists(dir_data)) {
  fs::dir_delete(dir_data)
}
fs::dir_create(dir_data)


# root cloud path
dir_gs <- "gs://clim_data_reg_useast1/nmme"

# date vector
date_vector <-
  seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month")

vars <- c("precipitation", "average_temperature")


# loop through models
for (mod_i in seq(nrow(df_sources))) {
  #
  mod <- df_sources$model[mod_i]
  message(str_glue("PROCESSING MODEL {mod} ({mod_i} / {nrow(df_sources)})"))

  # download data (precip and tas)
  ff <-
    map(vars |> set_names(), \(var) {
      #
      f <-
        rt_gs_list_files(str_glue("{dir_gs}/monthly/{mod}/{var}")) |>
        str_subset(str_flatten(
          seq(year(first(date_vector)), year(last(date_vector))),
          "|"
        ))

      f <-
        rt_gs_download_files(f, dir_data)

      return(f)
      #
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
        iwalk(\(f_m, i) {
          # load data
          ss <-
            f_m |>
            map(read_mdim)

          # pool all members from all years
          ss <-
            do.call(c, c(ss, along = "M"))

          # specify vars based on nmme var
          if (i == "average_temperature") {
            #
            distr <- lmom::pelgno
            f_base <- "average-temperature_mon_norm-params"
            dir_gs_clim <- "climatologies"
            #
          } else if (i == "precipitation") {
            #
            distr <- lmom::pelgam
            f_base <- "precipitation_mon_gamma-params"
            dir_gs_clim <- "climatologies_monthly_totals"

            # convert precip units: mm/d -> mm/month
            ss <-
              ss |>
              mutate(
                prec = round(units::set_units(prec, mm / month))
              )
            #
          }

          param_names <-
            sample(10, 10, replace = T) |>
            samlmu() |>
            distr() |>
            names()

          # split lead times
          ss_sliced <-
            seq(dim(ss)["L"]) |>
            map(\(lead_in) {
              ss |>
                slice(L, lead_in) |>
                units::drop_units()
            })

          # for each lead time
          r <-
            ss_sliced |>
            imap(\(s, lead_in) {
              message(str_glue(
                "      PROCESSING VARIABLE {i}  |  LEAD {lead_in}"
              ))

              # fit distributions
              s |>
                st_apply(
                  c(1, 2),
                  distr_params_apply,
                  param_names = param_names,
                  distribution = distr,
                  CLUSTER = cl,
                  .fname = "params"
                ) |>
                split("params")
            })

          r <-
            do.call(c, c(r, along = "L"))

          # result file name
          f <-
            str_glue("{dir_data}/nmme_{mod}_{f_base}_1991-2020_{mon}_leads-6.nc")

          # write to disk
          rt_write_nc(r, f)

          # move to bucket
          str_glue(
            "gcloud storage mv {f} gs://clim_data_reg_useast1/nmme/{dir_gs_clim}/{mod}/"
          ) |>
            system(ignore.stdout = T, ignore.stderr = T)
        })
    })

  # clean up
  walk(ff, \(f) walk(f, fs::file_delete))
}

stop_cluster(cl)

# clean up
fs::dir_delete(dir_data)
