# SCRIPT TO CALCULATE THE BASELINE (1991-2020) TAS
# AND PRECIP DISTRIBUTION PARAMETERS WITH NMME DATA

library(tidyverse)
library(stars)
library(furrr)
library(lmom)

plan(multicore, workers = parallelly::availableCores() - 1)


source("distribution_parameters/setup.R")
source("functions/general_tools.R")
source("functions/functions_drought.R")
source("functions/functions_distributions.R")
source("monitor_forecast/nmme_sources_df.R")


# root cloud path
dir_gs <- "gs://clim_data_reg_useast1/nmme"

# date vector
date_vector <-
  seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month")

vars <- c("precipitation", "average_temperature")

conv <-
  (units::set_units(1, month) / units::set_units(1, d)) |>
  units::drop_units()


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
        rt_gs_download_files(f, dir_data, quiet = T)

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
        iwalk(\(f_m, var) {
          # load data
          ss <-
            f_m |>
            future_map(read_mdim)

          # pool all members from all years
          ss <-
            do.call(c, c(ss, along = "M"))

          # specify vars based on nmme var
          if (var == "average_temperature") {
            #
            distribution <- pelgno
            f_base <- "average-temperature_mon_norm-params"
            dir_gs_clim <- "climatologies"
            #
          } else if (var == "precipitation") {
            #
            distribution <- pelgam
            f_base <- "precipitation_mon_gamma-params"
            dir_gs_clim <- "climatologies_monthly_totals"

            # convert precip units: mm/d -> mm/month
            ss <-
              ss |>
              units::drop_units() |>
              mutate(prec = prec * conv)
            #
          }

          param_names <-
            names(distribution(c(1, 0.1, 0.1, 0.1)))

          # split lead times
          ss_sliced <-
            seq(dim(ss)["L"]) |>
            map(\(i_lead) {
              ss |>
                slice(L, i_lead) |>
                units::drop_units()
            })

          # for each lead time
          r <-
            ss_sliced |>
            future_imap(\(s, i_lead) {
              # fit distributions
              s |>
                st_apply(
                  c(1, 2),
                  distr_params_apply,
                  param_names = param_names,
                  distribution = distribution,
                  # FUTURE = T,
                  .fname = "params"
                ) |>
                split("params")
            })

          r <-
            do.call(c, c(r, along = "L")) |>
            st_set_dimensions("L", seq(0, 6))
          # CHANGE VALUES OF L

          # result file name
          f <-
            str_glue("{dir_data}/nmme_{mod}_{f_base}_1991-2020_{mon}_leads-7.nc")

          # write to disk
          rt_write_nc(r, f)

          # move to bucket
          str_glue(
            "gcloud storage mv {f} {dir_gs}/{dir_gs_clim}/{mod}/"
          ) |>
            system(ignore.stdout = T, ignore.stderr = T)
        })
    })

  # clean up
  walk(ff, \(f) walk(f, fs::file_delete))
}
