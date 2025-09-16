# SCRIPT TO DOWNLOAD AND PRE-PROCESS NMME DATA TO GENERATE A FORECAST OF
# WATER BALANCE ANOMALIES. THE RESULTING FORECAST HAS A 6-MONTH LEAD AT
# A MONTHLY RESOLUTION.

# PRE-PROCESSING STEPS INCLUDE BIAS CORRECTION WITH ERA5 DATA.

# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.globals.maxSize = 4000 * 1024^2)
plan(multicore)


# load special functions
box::use(.. / functions / general_tools[...])
box::use(.. / functions / drought[...])

# load date to process
source("monitor_forecast/date_to_proc.R")
date_to_proc <- as.character(as_date(date_to_proc) + months(1))

# load nmme data source table
source("monitor_forecast/nmme_sources_df.R")

# temporary directory to save files
dir_tmp <- "/mnt/pers_disk/tmp"
# if (fs::dir_exists(dir_tmp)) fs::dir_delete(dir_tmp) # clean slate
fs::dir_create(dir_tmp)

# key bucket directories
dir_gs_era <- "gs://clim_data_reg_useast1/era5"
dir_gs_nmme <- "gs://clim_data_reg_useast1/nmme/climatologies"

# date to process plus 5 lead months
date_to_proc <- as_date(date_to_proc)
dates_fcst <- seq(date_to_proc, date_to_proc + months(5), by = "1 month")


# generate heat index constants
heat_vars <- heat_index_var_generator()


# ERA5 SECTION ----------------------------------------------------------------

# Download and prepare: (1) ERA5 distribution parameters and (2) X
# months of water balance prior to the date to process to apply an X-month
# rolling sum.

vars_era <-
  c(
    "total-precipitation",
    "2m-temperature",
    "water-balance-th-rollsum3",
    "water-balance-th-rollsum12"
  ) |>
  set_names(c("pr", "tas", "wb_rollsum3", "wb_rollsum12"))


# download ERA5 dist parameters files

ff_era <-
  map(vars_era, \(var) {
    f <-
      rt_gs_list_files(str_glue("{dir_gs_era}/climatologies")) |>
      str_subset(str_glue("_{var}_")) |>
      str_subset("1991-2020")

    # rearrange to follow order of dates_fcst
    # (instead of numerical)
    f <-
      str_sub(dates_fcst, -5, -4) |>
      map_chr(\(m) {
        f |> str_subset(str_glue("_{m}"))
      })

    # download
    f <-
      rt_gs_download_files(f, dir_tmp)

    return(f)
  })


# load ERA5 dist param files

era_params <-
  ff_era |>
  map(\(f) {
    s <-
      f |>
      set_names(f |> str_sub(-5, -4)) |>
      map(read_ncdf) |>
      suppressMessages()

    s <-
      do.call(c, c(s, along = "L")) |>
      st_set_dimensions("L", values = dates_fcst)

    return(s)
  })


# dates of the X wb months prior
dates_era <-
  c(3, 12) |>
  set_names() |>
  map(\(k) {
    seq(date_to_proc - months(k - 1), date_to_proc - months(1), by = "1 month")
  })


# calculate two wb months from tas and pr

era_Xmonths_wb <-
  map(dates_era, \(dd) {
    map(dd, \(d) {
      ss_var <-
        vars_era[1:2] |> # only pr and tas
        map(\(v) {
          f <-
            str_glue(
              "{dir_gs_era}/monthly_means/{str_replace(v, '-', '_')}/era5_{v}_mon_{d}.nc"
            )

          system(
            str_glue("gcloud storage cp {f} {dir_tmp}"),
            ignore.stdout = T,
            ignore.stderr = T
          )

          f <-
            str_glue("{dir_tmp}/{fs::path_file(f)}")

          r <-
            read_ncdf(f) |>
            suppressMessages() |>
            adrop()

          fs::file_delete(f)

          return(r)
        })

      s_wb <-
        wb_calculator_th(
          d,
          ss_var$tas |>
            setNames("tas") |>
            mutate(tas = tas |> units::set_units(degC)),
          ss_var$pr |>
            setNames("pr"),
          heat_vars
        )

      return(s_wb)
    })
  })


# NMME SECTION ----------------------------------------------------------------

# Downloads a month of NMME tas and precip data (with a 6-month lead). Bias-adjusts
# it with ERA5 distr parameters. Calculates water balance. Roll-sums with two
# previous ERA5 months. Calculates deviations (quantiles).

vars_nmme <- c("prec", "tref") |> set_names(c("pr", "tas"))


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
    imap(vars_nmme, \(var, v) {
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

      # download and read nmme forecast

      url <-
        nmme_url_generator(mod, date_to_proc, var, df = df_sources)

      f <-
        str_glue("{dir_tmp}/nmme_{mod}_{var_l}_mon_{date_to_proc}_plus5_pre.nc")

      a <- "a" # empty vector
      class(a) <- "try-error" # assign error class

      while (class(a) == "try-error") {
        a <-
          try(
            download.file(url, f, method = "wget", quiet = T)
          )

        if (class(a) == "try-error") {
          print(stringr::str_glue(
            "      download failed - waiting to retry..."
          ))
          Sys.sleep(3)
        }
      }

      fcst <-
        nmme_formatter(f, var) |>
        st_set_dimensions("L", values = dates_fcst)

      if (var == "prec") {
        fcst <-
          fcst |>
          mutate(prec = prec |> units::set_units(m / d))
      }

      f_fcst <- str_glue("nmme_{mod}_{var_l}_mon_{date_to_proc}_plus5.nc")
      f_fcst_dir <- str_glue("{dir_tmp}/{f_fcst}")

      rt_write_nc(fcst, f_fcst_dir)

      str_glue(
        "gcloud storage mv {f_fcst_dir} gs://clim_data_reg_useast1/nmme/monthly/{mod}/{str_replace(var_l, '-', '_')}/"
      ) |>
        system(ignore.stdout = T, ignore.stderr = T)

      fs::file_delete(f)

      # download and read model's distr params

      f_nmme_params <-
        case_when(
          var == "prec" ~
            str_glue(
              "nmme_{mod}_{var_l}_mon_gamma-params_1991-2020_{str_sub(date_to_proc, 6,7)}_plus5.nc"
            ),
          var == "tref" ~
            str_glue(
              "nmme_{mod}_{var_l}_mon_norm-params_1991-2020_{str_sub(date_to_proc, 6,7)}_plus5.nc"
            )
        )

      str_glue(
        "gcloud storage cp {dir_gs_nmme}/{mod}/{f_nmme_params} {dir_tmp}"
      ) |>
        system(ignore.stdout = T, ignore.stderr = T)

      nmme_params <-
        str_glue("{dir_tmp}/{f_nmme_params}") |>
        read_ncdf() |>
        suppressMessages() |>
        st_set_dimensions("L", values = dates_fcst)

      fs::file_delete(str_glue("{dir_tmp}/{f_nmme_params}"))

      ### BIAS CORRECTION -----------------------------------------------------

      # loop through lead months

      s_1var <-
        map(seq_along(dates_fcst), \(d_fcst_in) {
          # d_fcst_in <- 1

          print(str_glue("* * * LEAD {d_fcst_in} / {length(dates_fcst)}"))

          # era params for 1 lead month
          era_params_1mon <-
            era_params |>
            pluck(v) |>
            slice(L, d_fcst_in)

          # nmme params for 1 lead month
          nmme_params_1mon <-
            nmme_params |>
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
                          min(lmom::cdfgam(x[1], c(x[2], x[3])), 0.999)
                        }
                      } else if (var == "tref") {
                        lmom::cdfgno(x[1], c(x[2], x[3], x[4]))
                      }
                    }
                  },
                  .fname = "quantile",
                  FUTURE = T
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
                  .fname = {{ var }},
                  FUTURE = T
                )

              return(fcst_biasadj)
            })

          s_bias_adj_1mon <-
            do.call(c, c(s_bias_adj_1mon, along = "M"))

          return(s_bias_adj_1mon)
        })

      s_1var <-
        do.call(c, c(s_1var, along = "L"))

      return(s_1var)
    })

  # merge pr and tas

  s_1model <-
    do.call(c, unname(s_1model))

  write_rds(s_1model, str_glue("{dir_tmp}/bias-corr-vars_{mod}.rds"))
})


## CALCULATE WB AND ITS ANOMALIES ---------------------------------------------

print(str_glue("CALCULATING WB AND ANOMALIES..."))

plan(multicore, workers = 2)

walk(df_sources$model |> set_names(), \(mod) {
  message(" ")
  message(str_glue(
    "* MODEL {which(mod == df_sources$model)} / {nrow(df_sources)}"
  ))

  s_1model <-
    str_glue("{dir_tmp}/bias-corr-vars_{mod}.rds") |>
    read_rds()

  s_1model_wb <-
    # loop through members
    future_map(seq(dim(s_1model)["M"]), \(mem) {
      # mem = 1

      # print(str_glue("* * MEMBER {mem} / {dim(s_1model)['M']}"))

      # tas and pr data
      s_vars <-
        s_1model |>
        slice(M, mem)

      # calculate wb for 6 lead months
      ss_1mem <-
        map(seq(dim(s_vars)["L"]), \(d_fcst_in) {
          wb_calculator_th(
            dates_fcst[d_fcst_in],
            s_vars |>
              select(tref) |>
              slice(L, d_fcst_in) |>
              setNames("tas") |>
              mutate(
                tas = tas |> units::set_units(K) |> units::set_units(degC)
              ),
            s_vars |>
              select(prec) |>
              slice(L, d_fcst_in) |>
              setNames("pr") |>
              mutate(pr = pr |> units::set_units(m)),
            heat_vars
          )
        })

      # merge ERA 2 months wb with nmme 6 months
      s_wb_era_nmme <-
        era_Xmonths_wb |>
        map(\(e) do.call(c, c(c(e, ss_1mem), along = "L")))

      # rollsum
      s_wb_rolled <-
        s_wb_era_nmme |>
        imap(\(s, k) {
          k <- as.numeric(k)

          s |>
            st_apply(
              c(1, 2),
              \(x) {
                if (any(is.na(x))) {
                  rep(NA, length(x))
                } else {
                  # zoo::rollsum(x, k = 3, fill = NA, align = "right")
                  slider::slide_dbl(x, .f = sum, .before = k - 1, .complete = T)
                }
              },
              .fname = "L",
              FUTURE = F
            ) |>
            aperm(c(2, 3, 1)) |>
            slice(L, -seq(k - 1)) |> # remove traling 2 first months
            st_set_dimensions("L", values = dates_fcst) |>
            setNames(str_glue("wb_rollsum{k}"))
        })

      # calculate quantiles with ERA5 distr parameters
      s_wb_quantile <-
        s_wb_rolled |>
        imap(\(s, k) {
          p <- str_glue("wb_rollsum{k}")

          c(s, pluck(era_params, p)) |>
            merge() |>
            st_apply(
              c(1, 2, 3),
              \(x) {
                if (any(is.na(x))) {
                  NA
                } else {
                  lmom::cdfglo(x[1], c(x[2], x[3], x[4]))
                }
              },
              .fname = "perc",
              FUTURE = F
            ) |>
            setNames(str_glue("perc_{k}"))
        })

      # return(s_wb_quantile)
      do.call(c, s_wb_quantile |> unname())
    })

  s_1model_wb <-
    do.call(c, c(s_1model_wb, along = "M"))

  write_rds(s_1model_wb, str_glue("{dir_tmp}/wb-quantiles_{mod}.rds"))
})


# FINAL STATS

ff_wb_quantiles_all_models <-
  # str_glue("{dir_tmp}/wb-quantiles_{df_sources$model}.rds")                # **********
  fs::dir_ls(dir_tmp, regexp = "wb-quantiles_")

num_models <- length(ff_wb_quantiles_all_models)

wb_quantiles <-
  ff_wb_quantiles_all_models |>
  map(\(f) read_rds(f))

wb_quantiles <-
  do.call(c, c(wb_quantiles, along = "M"))


wb_quantiles_stats <-
  wb_quantiles |>
  st_apply(
    c(1, 2, 3),
    \(x) {
      if (all(is.na(x))) {
        # c(mean = NA, mode = NA, agree = NA, `5%` = NA, `20%` = NA, `50%` = NA, `80%` = NA, `95%` = NA)
        c(`5%` = NA, `20%` = NA, `50%` = NA, `80%` = NA, `95%` = NA)
      } else {
        # mean_perc <- mean(x, na.rm = T) |> round(2)
        #
        # # mode (most common decile)
        # mode_perc <-
        #   cut(x, seq(0,1,0.1), labels = seq(0,0.9,0.1)) |>
        #   table() |>
        #   which.max() |>
        #   names() |>
        #   as.numeric() # lower bound
        #
        #
        # # # how many members are in the same tercile as the mean
        # # upper_lim <- ceiling(mean_perc / (1/3)) * (1/3)
        # # agree <- mean(x < upper_lim & x > (upper_lim-(1/3)))
        #
        # # how many memebers are in the same quartile, centered on the (NOT:most common decile) median
        # # add 0.05 to center the bin
        # # agree <- round(mean(x < mean_perc+0.05+(1/4/2) & x > mean_perc+0.05-(1/4/2))*100)
        # # agree <- round(mean(x < mode_perc+0.2 & x > mode_perc-0.1)*100)
        # agree <- round(mean(x < median(x, na.rm = T)+0.15 & x > median(x, na.rm = T)-0.15)*100)

        q <- quantile(x, c(0.05, 0.2, 0.5, 0.8, 0.95), na.rm = T) |> round(2)

        # c(mean = mean_perc, mode = mode_perc+0.05, agree = agree, q)
        return(q)
      }
    },
    .fname = "stats"
  )


c(3, 12) |>
  iwalk(\(k, i) {
    f_name <- str_glue(
      "{dir_tmp}/nmme_ensemble_water-balance-perc-w{k}_mon_{date_to_proc}_plus5_{num_models}models.nc"
    )
    print(f_name)

    rt_write_nc(
      wb_quantiles_stats |> select(i) |> split("stats"),
      f_name,
      gatt_name = "source code",
      gatt_val = "https://github.com/carlosdobler/drought/drought_monitor"
    )

    "gcloud storage mv {f_name} gs://drought-monitor/forecast/" |>
      str_glue() |>
      system()
  })


# clean up
fs::dir_delete(dir_tmp)
