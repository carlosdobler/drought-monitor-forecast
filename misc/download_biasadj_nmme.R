#
# SCRIPT TO:
# 1. DOWNLOAD NMME DATA AND SAVE TO BUCKET
# 2. BIAS ADJUST AND SAVE RESULTS TO BUCKET
# SCRIPT BASED ON monitor_forecast/2_nmme_forecast_generator
# BUT RUN FOR ALL NMME DATA AVAILABLE AFTER 2020 (END OF BASELINE PERIOD;
# BASELINE PERIOD ALREADY DOWNLOADED AND FORMATTED WITH
# distribution_parameters/02_* SCRIPTS (BUT NOT BIAS ADJUSTED!)

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T, future.rng.onMisuse = "ignore")
plan(multicore)

# load special functions
source("functions/functions_drought.R")
source("functions/general_tools.R")

# create temp dir
dir_data <- "/mnt/pd-tmp-dist/tmp"
fs::dir_create(dir_data)

# key bucket directories
dir_gs_era <- "gs://clim_data_reg_useast1/era5"
dir_gs_nmme <- "gs://clim_data_reg_useast1/nmme"

# load nmme data source table
source("monitor_forecast/nmme_sources_df.R")

all_dates <-
  seq(as_date("1990-12-01"), as_date("2025-08-01"), by = "1 month") |>
  as.character()


# DIST PARAMS SECTION ----------------------------------------------------------------

# Download and prepare ERA5 distribution parameters

vars_era <-
  c(
    "total-precipitation",
    "2m-temperature"
  ) |>
  set_names(c("pr", "tas"))

ff_era <-
  map(vars_era, \(var) {
    #
    if (var == "total-precipitation") {
      dir_clim <- "climatologies_monthly_totals"
    } else {
      dir_clim <- "climatologies"
    }

    f <-
      rt_gs_list_files(str_glue("{dir_gs_era}/{dir_clim}")) |>
      str_subset(str_glue("_{var}_")) |>
      str_subset("1991-2020")

    # download
    f <-
      rt_gs_download_files(f, dir_data, quiet = T)

    return(f)
  })


# load ERA5 dist param files

era_params <-
  ff_era |>
  map(\(f) {
    #
    s <-
      f |>
      set_names(f |> str_sub(-5, -4)) |>
      map(read_mdim)

    return(s)
  })

ff_era |>
  walk(\(f) {
    fs::file_delete(f)
  })


vars_nmme <-
  c("prec", "tref") |>
  set_names(c("pr", "tas"))


# NMME SECTION ----------------------------------------------------------------

## BIAS CORRECT PRECIP AND TAS ------------------------------------------------

message(str_glue("BIAS CORRECTING..."))

# loop through models
for (mod_i in seq(nrow(df_sources))) {
  # mod_i = 1
  mod <- df_sources$model[mod_i]

  print(str_glue(" "))
  print(str_glue(
    "* MODEL {mod} ({mod_i} / {nrow(df_sources)})"
  ))

  # Download and prepare NMME distribution parameters
  ff_nmme <-
    map(vars_nmme, \(var) {
      #
      if (var == "prec") {
        var_l <- "precipitation"
        dir_clim <- "climatologies_monthly_totals"
      } else {
        var_l <- "average-temperature"
        dir_clim <- "climatologies"
      }

      f <-
        rt_gs_list_files(str_glue("{dir_gs_nmme}/{dir_clim}/{mod}")) |>
        str_subset(str_glue("_{var_l}_")) |>
        str_subset("1991-2020")

      # download
      f <-
        rt_gs_download_files(f, dir_data, quiet = T)

      return(f)
    })

  # load NMME dist param files

  nmme_params <-
    ff_nmme |>
    map(\(f) {
      #
      s <-
        f |>
        set_names(f |> str_sub(-13, -12)) |>
        map(read_mdim)

      return(s)
    })

  ff_nmme |>
    walk(\(f) {
      fs::file_delete(f)
    })

  # **********

  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  for (var_i in seq_along(vars_nmme)[1]) {
    #
    # var_i = 1
    var <- vars_nmme[var_i]
    v <- names(var)

    print(str_glue(
      "* * VARIABLE {which(var == vars_nmme)} / {length(vars_nmme)}"
    ))

    var_l <-
      case_when(
        var == "prec" ~ "precipitation",
        var == "tref" ~ "average-temperature"
      )

    # dist parameters for 1 variable
    era_params_1var <-
      era_params |>
      pluck(v)

    nmme_params_1var <-
      nmme_params |>
      pluck(v)

    # all dates available
    # (raw NMME monthly data)
    dates_raw <-
      str_glue(
        "gs://clim_data_reg_useast1/nmme/monthly/{mod}/{str_replace(var_l, '-', '_')}/*.nc"
      ) |>
      rt_gs_list_files() |>
      str_sub(-21, -12)

    # loop over dates
    for (date_ic in all_dates) {
      # date_ic <- all_dates[1]

      ### DOWNLOAD DATA ---------------------------------------------------------

      if (!date_ic %in% dates_raw) {
        print(str_glue(
          "    ...  downloading + formatting {date_ic}"
        ))

        url <-
          nmme_url_generator(mod, date_ic, var, df = df_sources)

        f <-
          str_glue("{dir_data}/nmme_{mod}_{var_l}_mon_ic-{date_ic}_leads-6_pre.nc")

        a <- "a" # empty vector
        class(a) <- "try-error" # assign error class
        attempt <- 1

        while (class(a) == "try-error" & attempt < 10) {
          a <-
            try(
              download.file(url, f, method = "wget", quiet = T)
            )

          if (class(a) == "try-error") {
            message("      download failed - waiting to retry...")
            attempt <- attempt + 1
            Sys.sleep(3)
          }
        }

        if (class(a) == "try-error") {
          message(
            "      download failed after 10 attempts - moving on to next variable or model..."
          )
          return(NULL)
        }

        # main object
        fcst <-
          nmme_formatter(f, var)

        # *****

        # save model data for future applications

        f_formatted <-
          f |> str_replace("_pre.nc$", ".nc")

        rt_write_nc(fcst, f_formatted)

        str_glue(
          "gcloud storage mv {f_formatted} gs://clim_data_reg_useast1/nmme/monthly/{mod}/{str_replace(var_l, '-', '_')}/"
        ) |>
          system(ignore.stdout = T, ignore.stderr = T)

        # *****

        fs::file_delete(f)
        #
      } else if (date_ic %in% dates_raw) {
        #
        print(str_glue(
          "    ...  downloading {date_ic} from bucket"
        ))

        f_nmme_raw <-
          "gs://clim_data_reg_useast1/nmme/monthly/{mod}/{str_replace(var_l, '-', '_')}/nmme_{mod}_{var_l}_mon_ic-{date_ic}_leads-6.nc" |>
          str_glue() |>
          rt_gs_download_files(dir_data, quiet = T)

        fcst <-
          f_nmme_raw |>
          read_mdim()

        fs::file_delete(f_nmme_raw)
      }

      ### BIAS CORRECTION -----------------------------------------------------

      if (var == "prec") {
        fcst <-
          fcst |>
          mutate(prec = round(units::set_units(prec, mm / month)))
      }

      # *****

      s_1var <-
        map(seq(6), \(d_fcst_in) {
          # d_fcst_in <- 1

          message(" ")
          message(
            str_glue(
              "* * * BIAS CORRECTING LEAD {d_fcst_in} / 6 :: {date_ic} :: MODEL {mod_i} / {nrow(df_sources)}"
            )
          )

          tictoc::tic("      ... done with lead")

          month_lead <- month(as_date(date_ic) + months(d_fcst_in))

          # era params for 1 lead month
          era_params_1mon <-
            era_params_1var |>
            pluck(month_lead)

          # nmme params for 1 lead month
          nmme_params_1mon <-
            nmme_params_1var |>
            pluck(month(as_date(date_ic))) |>
            slice(L, d_fcst_in)

          # forecast for 1 lead month
          fcst_1mon <-
            fcst |>
            units::drop_units() |>
            slice(L, d_fcst_in)

          # bundle each nmme member with nmme dist parameters
          ss_mems <-
            # loop through members
            map(seq(dim(fcst_1mon)[3]), \(mem) {
              fcst_1mon |>
                slice(M, mem) |>
                c(nmme_params_1mon) |>
                merge()
            })

          # calculate quantiles based on dist parameters
          ss_quantiles <-
            map(ss_mems, \(s) {
              s |>
                st_apply(
                  c(1, 2),
                  \(x) {
                    if (any(is.na(x))) {
                      NA
                    } else {
                      if (var == "prec") {
                        if (x[2] == -9999) {
                          # no precip in historical period
                          NA
                        } else {
                          # a quantile of 1 or of 0 throws an error when converting to level
                          max(min(lmom::cdfgam(x[1], c(x[2], x[3])), 0.999), 0.001)
                        }
                      } else if (var == "tref") {
                        max(min(lmom::cdfgno(x[1], c(x[2], x[3], x[4])), 0.999), 0.001)
                      }
                    }
                  },
                  .fname = "quantile"
                ) |>
                #
                # resample to ERA5 resolution
                # first warp to avoid NAs in row 1439
                #
                st_warp(st_as_stars(
                  st_bbox(
                    c(
                      xmin = -180.125,
                      ymin = -90.125,
                      xmax = 179.875,
                      ymax = 90.125
                    ),
                    crs = 4326
                  ),
                  dx = 0.25
                )) |>
                st_warp(era_params_1mon)
            })

          # bundle quantile maps with era parameters
          ss_quantiles <-
            map(ss_quantiles, \(s) {
              s |>
                c(era_params_1mon) |>
                merge()
            })

          tictoc::tic(str_glue("      ... {length(ss_quantile)} members bias adjusted"))
          if (var == "prec") {
            ss_ba <-
              map(ss_quantiles, \(s) {
                s |>
                  st_apply(
                    c(1, 2),
                    \(x) {
                      if (any(is.na(x))) {
                        NA_integer_
                      } else if (x[2] == -9999) {
                        0L
                      } else {
                        round(lmom::quagam(x[1], c(x[2], x[3])))
                      }
                    },
                    FUTURE = T,
                    .fname = {{ var }}
                  )
              })
          } else if (var == "tref") {
            ss_ba <-
              map(ss_quantiles, \(s) {
                s |>
                  st_apply(
                    c(1, 2),
                    \(x) {
                      if (any(is.na(x))) {
                        NA
                      } else {
                        lmom::quagno(x[1], c(x[2], x[3], x[4]))
                      }
                    },
                    FUTURE = T,
                    .fname = {{ var }}
                  )
              })
          }

          tictoc::toc()
          tictoc::toc()
          return(ss_ba)
        })

      s_1var <-
        s_1var |>
        map(\(s) do.call(c, c(s, along = "M")))

      s_1var <-
        do.call(c, c(s_1var, along = "L"))

      # *****
      # save ba model data for future applications

      v_name <- names(s_1var)
      v_un <- units::deparse_unit(pull(fcst))

      s_1var_f <-
        s_1var |>
        aperm(c(1, 2, 4, 3)) |>
        mutate(!!sym(v_name) := units::set_units(!!sym(v_name), !!v_un))

      f_fcst <-
        str_glue("{dir_data}/nmme_{mod}_{var_l}_mon_ic-{date_ic}_leads-6_biasadj.nc")

      # ONLY PREC!!!!!!!!!!

      s_1var_f <-
        s_1var_f |>
        mutate(prec = as.integer(prec))

      rt_write_nc(s_1var_f, f_fcst)

      str_glue(
        "gcloud storage mv {f_fcst} gs://clim_data_reg_useast1/nmme/monthly_totals/{mod}/{str_replace(var_l, '-', '_')}_biasadj/"
      ) |>
        system(ignore.stdout = T, ignore.stderr = T)
      # }
    }
  }
}

fs::dir_delete(dir_data)
