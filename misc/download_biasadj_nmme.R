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
library(mirai)

daemons(parallel::detectCores() - 1)

# load special functions
source("functions/functions_drought.R")
source("functions/general_tools.R")

# create temp dir
dir_data <- "/mnt/pd-tmp-monitor/tmp-monitor"
fs::dir_create(dir_data)

# key bucket directories
dir_gs_era <- "gs://clim_data_reg_useast1/era5"
dir_gs_nmme <- "gs://clim_data_reg_useast1/nmme/climatologies"

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
    f <-
      rt_gs_list_files(str_glue("{dir_gs_era}/climatologies")) |>
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


# Download and prepare NMME distribution parameters

vars_nmme <-
  c("prec", "tref") |>
  set_names(c("pr", "tas"))

ff_nmme <-
  map(df_sources$model |> set_names(), \(mod) {
    map(vars_nmme, \(var) {
      #
      var_l <-
        case_when(
          var == "prec" ~ "precipitation",
          var == "tref" ~ "average-temperature"
        )

      f <-
        rt_gs_list_files(str_glue("{dir_gs_nmme}/{mod}")) |>
        str_subset(str_glue("_{var_l}_")) |>
        str_subset("1991-2020")

      # download
      f <-
        rt_gs_download_files(f, dir_data, quiet = T)

      return(f)
    })
  })

# load NMME dist param files

nmme_params <-
  ff_nmme |>
  map(\(ff_mod) {
    ff_mod |>
      map(\(f) {
        #
        s <-
          f |>
          set_names(f |> str_sub(-11, -10)) |>
          map(read_mdim)

        return(s)
      })
  })


ff_nmme |>
  walk(\(ff_mod) {
    ff_mod |>
      walk(\(f) {
        fs::file_delete(f)
      })
  })


# NMME SECTION ----------------------------------------------------------------

## BIAS CORRECT PRECIP AND TAS ------------------------------------------------

message(str_glue("BIAS CORRECTING..."))

# loop through models
walk(df_sources$model |> set_names(), \(mod) {
  # mod <- df_sources$model[1]

  print(str_glue(" "))
  print(str_glue(
    "* MODEL {which(mod == df_sources$model)} / {nrow(df_sources)}"
  ))

  s_1model <-
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    iwalk(vars_nmme[1], \(var, v) {
      # var = "prec"
      # v = "pr"

      # var = "tref"
      # v = "tas"

      print(str_glue(
        "* * VARIABLE {which(var == vars_nmme)} / {length(vars_nmme)}"
      ))

      var_l <-
        case_when(
          var == "prec" ~ "precipitation",
          var == "tref" ~ "average-temperature"
        )

      ### DOWNLOAD AND LOAD FORECAST AND NMME DISTRIBUTION PARAMS ------------

      dates_raw <-
        rt_gs_list_files(str_glue(
          "gs://clim_data_reg_useast1/nmme/monthly/{mod}/{str_replace(var_l, '-', '_')}/*.nc"
        )) |>
        str_sub(-21, -12)

      dates_biasadj <-
        rt_gs_list_files(str_glue(
          "gs://clim_data_reg_useast1/nmme/monthly/{mod}/{str_replace(var_l, '-', '_')}_biasadj/*.nc"
        )) |>
        str_sub(-29, -20)

      # download and read nmme forecast

      for (date_ic in all_dates) {
        # date_ic <- all_dates[1]

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

          if (var == "prec") {
            fcst <-
              fcst |>
              mutate(prec = prec |> units::set_units(m / d)) # match ERA5 units
          }

          fs::file_delete(f)
        }

        ### BIAS CORRECTION -----------------------------------------------------

        # loop through lead months

        if (!date_ic %in% dates_biasadj) {
          #
          if (date_ic %in% dates_raw) {
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

          s_1var <-
            map(seq(6), \(d_fcst_in) {
              # d_fcst_in <- 1

              print(str_glue("* * * LEAD {d_fcst_in} / 6 :: {date_ic}"))

              month_lead <- month(as_date(date_ic) + months(d_fcst_in))

              # era params for 1 lead month
              era_params_1mon <-
                era_params |>
                pluck(v) |>
                pluck(month_lead)

              # nmme params for 1 lead month
              nmme_params_1mon <-
                nmme_params |>
                pluck(mod) |>
                pluck(v) |>
                pluck(month(as_date(date_ic))) |>
                slice(L, d_fcst_in)

              # forecast for 1 lead month
              fcst_1mon <-
                fcst |>
                units::drop_units() |>
                slice(L, d_fcst_in)

              # bias-adjust 1 lead month
              s_bias_adj_1mon <-
                # loop through members
                map(seq(dim(fcst_1mon)[3]), \(mem) {
                  #
                  print(str_glue("* * * * MEMBER {mem} / {dim(fcst_1mon)[3]}"))

                  # calculate quantile of nmme data
                  # based on nmme distributions

                  nmme_quantile <-
                    fcst_1mon |>
                    slice(M, mem) |>
                    c(nmme_params_1mon) |>
                    merge() |>
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
                    )

                  nmme_quantile <-
                    nmme_quantile |>
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

                  # quantile mapping with ERA5
                  # distributions

                  fcst_biasadj <-
                    nmme_quantile |>
                    c(era_params_1mon) |>
                    merge() |>
                    st_apply(
                      c(1, 2),
                      \(x) {
                        if (any(is.na(x))) {
                          NA
                        } else if (x[2] == -9999) {
                          0
                        } else {
                          if (var == "prec") {
                            lmom::quagam(x[1], c(x[2], x[3]))
                          } else if (var == "tref") {
                            lmom::quagno(x[1], c(x[2], x[3], x[4]))
                          }
                        }
                      },
                      .fname = {{ var }}
                    )

                  return(fcst_biasadj)
                })

              s_bias_adj_1mon <-
                do.call(c, c(s_bias_adj_1mon, along = "M"))

              return(s_bias_adj_1mon)
            })

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

          rt_write_nc(s_1var_f, f_fcst)

          str_glue(
            "gcloud storage mv {f_fcst} gs://clim_data_reg_useast1/nmme/monthly/{mod}/{str_replace(var_l, '-', '_')}_biasadj/"
          ) |>
            system(ignore.stdout = T, ignore.stderr = T)
        }
      }
    })
})


fs::dir_delete(dir_data)
