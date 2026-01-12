# SCRIPT TO DOWNLOAD AND PRE-PROCESS NMME DATA TO GENERATE A FORECAST OF
# WATER BALANCE ANOMALIES. THE RESULTING FORECAST HAS A 6-MONTH LEAD AT
# A MONTHLY RESOLUTION.

# PRE-PROCESSING STEPS INCLUDE BIAS CORRECTION WITH ERA5 DATA.

message("***** PROCESSING FORECAST *****")

# SETUP -----------------------------------------------------------------------

# load nmme data source table
source("monitor_forecast/nmme_sources_df.R")

# date to process = IC; dates of forecast = 6 lead months
date_ic <- as_date(date_to_proc)
dates_fcst <- seq(date_ic + months(1), date_ic + months(6), by = "1 month")


# ERA5 SECTION ----------------------------------------------------------------

# Download and prepare: (1) ERA5 distribution parameters and (2) X
# months of water balance prior to the date to process to apply an X-month
# rolling sum.

message("Preparing ERA5 data...")

vars_era <-
  c(
    "total-precipitation",
    "2m-temperature",
    str_glue("water-balance-hamon-rollsum{winds}")
  ) |>
  set_names(c("pr", "tas", str_glue("wb_rollsum{winds}")))

clim_dir <-
  c(
    "climatologies_monthly_totals",
    "climatologies",
    "climatologies_monthly_totals",
    "climatologies_monthly_totals"
  )


# download ERA5 dist parameters files

ff_era <-
  map2(vars_era, clim_dir, \(var, dir) {
    #
    f <-
      rt_gs_list_files(str_glue("{dir_gs_era}/{dir}")) |>
      str_subset(str_glue("_{var}_mon")) |>
      str_subset("1991-2020")

    # rearrange to follow order of dates_fcst
    # (instead of alphanumerical)
    f <-
      str_sub(c(date_ic, dates_fcst), -5, -4) |>
      map_chr(\(m) {
        f |> str_subset(str_glue("_{m}"))
      })

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
    f |>
      set_names(f |> str_sub(-5, -4)) |>
      map(read_mdim)

    # s <-
    #   do.call(c, c(s, along = "L")) |>
    #   st_set_dimensions("L", values = dates_fcst)

    # return(s)
  })


# dates of wb from ERA5 needed for rolling sum (prior to fcst date)
dates_era <-
  winds |>
  set_names() |>
  map(\(k) {
    seq(dates_fcst[1] - months(k - 1), dates_fcst[1] - months(1), by = "1 month")
  })


# wb months for tas and pr for rolling sums
# already available from monitor script: s_era_wb

# NMME SECTION ----------------------------------------------------------------

# Downloads a month of NMME tas and precip data (with a 6-month lead). Bias-adjusts
# it with ERA5 distr parameters. Calculates water balance. Roll-sums with two
# previous ERA5 months. Calculates deviations (quantiles).

message("Processing NMME data...")

vars_nmme <-
  c("prec", "tref") |>
  set_names(c("pr", "tas"))


## BIAS CORRECT PRECIP AND TAS ------------------------------------------------

# loop through models
walk(df_sources$model |> tail(-2) |> set_names(), \(mod) {
  # mod <- df_sources$model[3]

  print(str_glue(" "))
  print(str_glue(
    "* model {which(mod == df_sources$model)} / {nrow(df_sources)}"
  ))

  ss_nmme_ba <-
    imap(vars_nmme, \(var, v) {
      # var = "prec"
      # v = "pr"

      # var = "tref"
      # v = "tas"

      message(str_glue(
        "* * bias correcting variable {which(var == vars_nmme)} / {length(vars_nmme)}"
      ))

      var_l <-
        case_when(
          var == "prec" ~ "precipitation",
          var == "tref" ~ "average-temperature"
        )

      ### DOWNLOAD AND LOAD FORECAST AND NMME DISTRIBUTION PARAMS ------------

      # download and read nmme forecast

      url <-
        nmme_url_generator(mod, date_ic, var, df = df_sources)

      f <-
        str_glue("{dir_data}/nmme_{mod}_{var_l}_mon_ic-{date_ic}_leads-7_pre.nc")

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
        message("      download failed after 10 attempts - moving on to next variable or model...")
        return(NULL)
      }

      fcst <-
        nmme_formatter(f, var)

      if (all(is.na(pull(fcst)))) {
        message("      file with no data - moving on to next variable or model...")
        fs::file_delete(f)
        return(NULL)
      }

      # *****
      # save model data for future applications

      f_formatted <-
        f |> str_replace("_pre.nc$", ".nc")

      rt_write_nc(fcst, f_formatted)

      str_glue(
        "gcloud storage mv {f_formatted} {dir_gs_nmme}/monthly/{mod}/{str_replace(var_l, '-', '_')}/"
      ) |>
        system(ignore.stdout = T, ignore.stderr = T)

      # *****

      # # remove first lead month
      # fcst <-
      #   fcst |>
      #   filter(L %in% seq(1, 6))

      # convert units
      if (var == "prec") {
        #
        fcst <-
          fcst |>
          units::drop_units() |>
          mutate(
            prec = prec * conv,
            prec = units::set_units(prec, mm / month)
          )
      }

      fs::file_delete(f)

      # download and read model's distr params

      f_nmme_params <-
        case_when(
          var == "prec" ~
            str_glue(
              "{dir_gs_nmme}/climatologies_monthly_totals/{mod}/nmme_{mod}_{var_l}_mon_gamma-params_1991-2020_{str_sub(date_ic, 6,7)}_leads-7.nc"
            ),
          var == "tref" ~
            str_glue(
              "{dir_gs_nmme}/climatologies/{mod}/nmme_{mod}_{var_l}_mon_norm-params_1991-2020_{str_sub(date_ic, 6,7)}_leads-7.nc"
            )
        )

      f_nmme_params <-
        f_nmme_params |>
        rt_gs_download_files(dir_data, quiet = T)

      nmme_params <-
        f_nmme_params |>
        read_mdim()

      # # remove first lead month
      # nmme_params <-
      #   nmme_params |>
      #   filter(L %in% seq(1, 6))

      fs::file_delete(f_nmme_params)

      # loop through lead months

      s_1var <-
        map(seq(dim(fcst)["L"]), \(d_fcst_in) {
          # d_fcst_in <- 1

          print(str_glue("* * * lead {d_fcst_in-1}"))

          # month_lead <- month(as_date(date_ic) + months(d_fcst_in))

          # era params for 1 lead month
          era_params_1mon <-
            era_params |>
            pluck(v) |>
            pluck(d_fcst_in)

          # nmme params for 1 lead month
          nmme_params_1mon <-
            nmme_params |>
            slice(L, d_fcst_in)

          # forecast for 1 lead month
          fcst_1mon <-
            fcst |>
            units::drop_units() |>
            slice(L, d_fcst_in)

          # bundle each nmme member with nmme dist parameters
          ss_mems <-
            # loop through members
            map(seq(dim(fcst_1mon)["M"]), \(mem) {
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
                        if (x[2] == -99999) {
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

          # calculate bias-adjusted levels
          if (var == "prec") {
            ss_ba <-
              future_map(ss_quantiles, \(s) {
                s |>
                  st_apply(
                    c(1, 2),
                    \(x) {
                      if (any(is.na(x))) {
                        NA
                      } else if (x[2] == -99999) {
                        0
                      } else {
                        lmom::quagam(x[1], c(x[2], x[3]))
                      }
                    },
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
                    .fname = {{ var }}
                  )
              })
          }

          do.call(c, c(ss_ba, along = "M"))
        })

      # *****
      # save ba model data for future applications

      v_name <- names(s_1var[[1]])
      v_un <- units::deparse_unit(pull(fcst))

      s_1var_f <-
        do.call(c, c(s_1var, along = "L")) |>
        st_set_dimensions("L", values = seq(0, 6)) |>
        aperm(c("longitude", "latitude", "L", "M")) |>
        mutate(!!sym(v_name) := units::set_units(!!sym(v_name), !!v_un))

      f_fcst <-
        str_glue("{dir_data}/nmme_{mod}_{var_l}_mon_ic-{date_ic}_leads-7_biasadj.nc")

      rt_write_nc(s_1var_f, f_fcst)

      dir_month <-
        case_when(
          var == "prec" ~ "monthly_totals",
          var == "tref" ~ "monthly"
        )

      str_glue(
        "gcloud storage mv {f_fcst} gs://clim_data_reg_useast1/nmme/{dir_month}/{mod}/{str_replace(var_l, '-', '_')}_biasadj/"
      ) |>
        system(ignore.stdout = T, ignore.stderr = T)

      # *****

      return(s_1var)
    })

  # remove lead 0 (first)

  ss_nmme_ba <-
    ss_nmme_ba |>
    map(\(s) {
      s[-1]
    })

  # **********************************************

  # CALCULATE WB

  message(str_glue(
    "* * calculating wb"
  ))

  ss_pet <-
    future_map2(dates_fcst, ss_nmme_ba$tas, \(d, s) {
      pet_calculator_hamon(
        d,
        s |>
          mutate(tref = units::set_units(tref, K))
      )
    })

  ss_pet <-
    ss_pet |>
    map(\(s) {
      s |>
        units::drop_units() |>
        mutate(pet = pet * conv)
    })

  ss_nmme_wb <-
    map2(ss_nmme_ba$pr, ss_pet, \(s_pr, s_pet) {
      s <- s_pr - s_pet
      s |>
        setNames("wb")
    })

  # split by members
  ss_nmme_wb <-
    map(ss_nmme_wb, \(s) {
      map(seq(dim(s)["M"]), \(mem) {
        slice(s, "M", mem)
      })
    }) |>
    transpose()

  # calculate wb quantiles

  walk(winds, \(k) {
    #
    message(str_glue("* * calculating percentiles window {k}"))

    era_params_wb_rolled <-
      era_params |>
      pluck(str_glue("wb_rollsum{k}")) |>
      _[-1]

    # merge wb nmme with wb era5 prior months
    # (list of members)
    ss_era_nmme_wb <-
      ss_nmme_wb |>
      map(\(ss_mem) {
        c(
          s_era_wb |>
            units::drop_units() |>
            filter(time %in% (dates_era |> pluck(as.character(k)))),
          do.call(c, c(ss_mem, along = "time")) |>
            st_set_dimensions("time", values = dates_fcst)
        )
      })

    ss_era_nmme_wb_rolled <-
      ss_era_nmme_wb |>
      future_map(\(s) {
        #
        s |>
          st_apply(
            c(1, 2),
            \(x) {
              r <- slider::slide_sum(x, before = k - 1, complete = T)
              tail(r, -k + 1)
            },
            .fname = "time"
          ) |>
          aperm(c(2, 3, 1))
      })

    # split by lead times and merge with parameters
    ss_wb_perc <-
      ss_era_nmme_wb_rolled |>
      map(\(s_wb) {
        seq(dim(s_wb)["time"]) |>
          map(\(l) {
            c(
              s_wb |>
                slice(time, l),
              era_params_wb_rolled[[l]]
            ) |>
              merge()
          })
      }) |>

      # calculate percentiles
      imap(\(ss_mem, i) {
        message(str_glue("* * * member {i} / {length(ss_era_nmme_wb)}"))

        ss_wb_perc_1mem <-
          ss_mem |>
          future_map(\(s) {
            s |>
              st_apply(
                c(1, 2),
                \(x) {
                  if (any(is.na(x))) {
                    NA
                  } else {
                    max(min(lmom::cdfglo(x[1], c(x[2], x[3], x[4])), 0.999), 0.001)
                  }
                },
                .fname = "perc"
              )
          })

        do.call(c, c(ss_wb_perc_1mem, along = "L"))
      })

    do.call(c, c(ss_wb_perc, along = "M")) |>
      write_rds(str_glue("{dir_data}/wb-quantiles-w{k}_{mod}.rds"))
  })
})


# FINAL STATS

message("Calculating ensemble stats...")

ff_wb_quantiles <-
  fs::dir_ls(dir_data, regexp = "wb-quantiles")

wb_quantiles <-
  winds |>
  set_names() |>
  map(\(k) {
    ff_wb_quantiles |>
      str_subset(str_glue("-w{k}_")) |>
      map(read_rds)
  })

# remove empty models
wb_quantiles <-
  wb_quantiles |>
  map(\(ss) {
    ss[map_lgl(ss, \(s) is(s, "stars"))]
  })

wb_quantiles <-
  wb_quantiles |>
  map(\(ss) {
    do.call(c, c(ss, along = "M"))
  })

wb_quantiles |>
  iwalk(\(ss, k) {
    message(str_glue("* window {k}"))
    r <-
      ss |>
      st_apply(
        c("longitude", "latitude", "L"),
        \(x) {
          if (all(is.na(x))) {
            c(`5%` = NA, `20%` = NA, `50%` = NA, `80%` = NA, `95%` = NA)
          } else {
            q <- quantile(x, c(0.05, 0.2, 0.5, 0.8, 0.95), na.rm = T) |> round(2)
            return(q)
          }
        },
        .fname = "stats"
      ) |>
      split("stats") |>
      st_set_dimensions("L", values = dates_fcst)

    f_name <-
      str_glue(
        "{dir_data}/nmme_ensemble_water-balance-perc-w{k}_mon_ic-{date_ic}_leads-6.nc"
      )

    rt_write_nc(r, f_name)

    "gcloud storage mv {f_name} gs://drought-monitor/forecast/" |>
      str_glue() |>
      system(ignore.stdout = T, ignore.stderr = T)
  })

ff_era |>
  walk(fs::file_delete)

ff_wb_quantiles |>
  fs::file_delete()
