# SCRIPT TO CALCULATE DROUGHT (WB ANOMALY) WITH ERA5 FOR 1 MONTH
# BASED ON A MODIFIED SPEI METHODOLOGY
# USES THORNWHAITES FORMULATION TO CALCULATE PET

library(tidyverse)
library(stars)
library(mirai)

daemons(parallel::detectCores() - 1)


# special functions
source("functions/drought.R")
source("functions/general_tools.R")

# load script params:
# date to process and temporary data directory
source("monitor_forecast/0_params.R")

# root bucket dir
dir_gs <- "gs://clim_data_reg_useast1/era5"

fs::dir_create(dir_data)


# loop through integration windows
for (k in winds) {
  message(str_glue("*** PROCESSING {date_to_proc} (window = {k}) ***"))

  # extract what wb quantiles dates have already been processed
  existing_dates <-
    rt_gs_list_files(str_glue("gs://drought-monitor/historical")) |>
    str_subset(".nc$") |>
    str_subset(str_glue("-w{k}_")) |>
    str_sub(-13, -4)

  if (date_to_proc %in% existing_dates) {
    # loop to next integration window if date already exists

    message("THIS DATE HAS ALREADY BEEN PROCESSED! Skipping process")
    next
    #
  } else {
    # start process if date does not exist

    # vector of all dates necessary to calculate rolling sum
    pre_dates_n <-
      seq(
        as_date(date_to_proc) - months(k - 1),
        as_date(date_to_proc),
        by = "1 month"
      )

    # if this is not the first window, check what wb dates are in dir_data
    if (k == winds[1]) {
      pre_dates <- pre_dates_n
    } else {
      existing_dates_wb_in_disk <-
        fs::dir_ls(dir_data) |>
        str_subset("era5_water-balance-th_mon_") |>
        str_sub(-13, -4)

      # update pre_dates:
      # only dates not in disk
      pre_dates <-
        pre_dates_n[!pre_dates_n %in% existing_dates_wb_in_disk]
    }

    # check if necessary wb dates are available in bucket
    existing_dates_wb <-
      rt_gs_list_files(str_glue(
        "{dir_gs}/monthly_means/water_balance_th/*.nc"
      )) |>
      str_sub(-13, -4)

    # existing_dates_wb <- existing_dates_wb |> head(-2)

    if (any(pre_dates %in% existing_dates_wb)) {
      # if any wb date is available, download it from bucket

      existing_pre_dates_wb <-
        pre_dates[pre_dates %in% existing_dates_wb]

      existing_pre_dates_wb |>
        walk(\(d) {
          message(str_glue("   Downloading wb {d} from bucket"))

          f <- str_glue("era5_water-balance-th_mon_{d}.nc")

          rt_gs_download_files(
            str_glue("{dir_gs}/monthly_means/water_balance_th/{f}"),
            dir_data,
            quiet = T
          )
        })
    }

    # This section runs if not all wb pre-dates are available in bucket or disk
    if (any(!pre_dates %in% existing_dates_wb)) {
      missing_pre_dates_wb <-
        pre_dates[!pre_dates %in% existing_dates_wb]

      # identify missing wb dates that are available as tas and precip (wb inputs)
      existing_pre_dates_inputs <-
        c("total_precipitation", "2m_temperature") |>
        set_names() |>
        map(\(var) {
          existing_dates_1_input <-
            rt_gs_list_files(str_glue("{dir_gs}/monthly_means/{var}")) |>
            str_sub(-13, -4)

          # existing_dates_1_input <- existing_dates_1_input |> head(-1)

          # missing wb: also missing pr and/or tas?
          # (boolean vector of length: missing wb months == TRUE)
          missing_pre_dates_wb[missing_pre_dates_wb %in% existing_dates_1_input]
        })

      if (length(unlist(existing_pre_dates_inputs)) > 0) {
        # if there are dates available as inputs, download them

        existing_pre_dates_inputs |>
          iwalk(\(d, var) {
            message(str_glue("   Downloading {var} {d} from bucket"))

            f <- str_glue("era5_{str_replace_all(var, '_', '-')}_mon_{d}.nc")

            rt_gs_download_files(
              str_glue("{dir_gs}/monthly_means/{var}/{f}"),
              dir_data,
              quiet = T
            )
          })
      }

      # This section runs if there are missing dates as inputs in buckets
      # Downloads them from cds
      if (length(unlist(existing_pre_dates_inputs)) < length(missing_pre_dates_wb) * 2) {
        # k*2 = 2 inputs (pr and tas) x the number of missing (needed) dates

        missing_pre_dates_inputs <-
          existing_pre_dates_inputs |>
          map(\(var) {
            missing_pre_dates_wb[!missing_pre_dates_wb %in% var]
          })

        reticulate::py_require("cdsapi")
        cdsapi <- reticulate::import("cdsapi")

        missing_pre_dates_inputs |>
          iwalk(\(d, var) {
            message(str_glue("   Downloading {var} {d} from cds"))

            f <- str_glue("era5_{str_replace_all(var, '_', '-')}_mon_{d}.nc")

            a <- "a" # empty vector
            class(a) <- "try-error" # assign error class

            while (class(a) == "try-error") {
              a <-
                try(
                  cdsapi$Client()$retrieve(
                    name = "reanalysis-era5-single-levels-monthly-means",

                    request = reticulate::dict(
                      format = "netcdf",
                      product_type = "monthly_averaged_reanalysis",
                      variable = var,
                      year = year(d),
                      month = str_pad(month(d), 2, "left", "0"),
                      time = "00:00"
                    ),

                    target = str_glue("{dir_data}/{f}")
                  )
                )

              if (class(a) == "try-error") {
                message("      waiting to retry...")
                Sys.sleep(3)
              }
            }

            # upload to bucket
            str_glue("gcloud storage cp {dir_data}/{f} {dir_gs}/monthly_means/{var}/") |>
              system(ignore.stdout = T, ignore.stderr = T)
          })
      }

      # calculate water balance for missing dates

      # heat index constants
      heat_vars <- heat_index_var_generator()

      # wb
      missing_pre_dates_wb |>
        walk(\(d) {
          # for each month

          # load temperature
          s_tas <-
            read_ncdf(str_glue(
              "{dir_data}/era5_2m-temperature_mon_{as_date(d)}.nc"
            )) |>
            suppressMessages() |>
            adrop()

          # load precipitation
          s_pr <-
            read_ncdf(str_glue(
              "{dir_data}/era5_total-precipitation_mon_{as_date(d)}.nc"
            )) |>
            suppressMessages() |>
            adrop()

          # calculate wb
          s_wb <-
            wb_calculator_th(
              d,

              s_tas |>
                setNames("tas") |>
                mutate(tas = tas |> units::set_units(degC)),

              s_pr |>
                setNames("pr"),

              heat_vars
            )

          # save to disk
          f_res <- str_glue("{dir_data}/era5_water-balance-th_mon_{d}.nc")
          rt_write_nc(s_wb, f_res)

          # upload to bucket
          str_glue(
            "gcloud storage cp {f_res} {dir_gs}/monthly_means/water_balance_th/"
          ) |>
            system(ignore.stdout = T, ignore.stderr = T)
        })
    }

    #
    #
    #

    # load all wb files
    s_wb <-
      pre_dates_n |>
      map(\(d) {
        str_glue("{dir_data}/era5_water-balance-th_mon_{d}.nc") |>
          read_ncdf() |>
          suppressMessages()
      })

    # concatenate all months
    s_wb <-
      do.call(c, c(s_wb, along = "time"))

    # aggregate (rollsum)
    s_wb_rolled <-
      s_wb |>
      st_apply(c(1, 2), sum, .fname = str_glue("wb_rollsum{k}"))

    message(str_glue("   Calculating anomalies"))

    # import baseline distribution parameters

    f_distr <-
      str_glue(
        "era5_water-balance-th-rollsum{k}_mon_log-params_1991-2020_{str_pad(month(as_date(date_to_proc)), 2, 'left', '0')}.nc"
      )

    s_dist_params <-
      rt_gs_download_files(
        str_glue("{dir_gs}/climatologies/{f_distr}"),
        dir_data,
        quiet = T
      ) |>
      read_ncdf() |>
      suppressMessages()

    # calculate quantile

    s_perc <-
      c(s_wb_rolled, s_dist_params) |>
      merge() |>
      st_apply(
        c(1, 2),
        \(x) {
          if (any(is.na(x))) {
            NA
          } else {
            lmom::cdfglo(x[1], c(x[2], x[3], x[4]))
          }
        },
        # FUTURE = T,
        .fname = "perc"
      )

    # check with spei csic
    # s_perc |>
    #   st_warp(st_as_stars(st_bbox(), dx = 0.25, values = NA)) |>
    #   as_tibble() |>
    #   ggplot(aes(x, y, fill = perc)) +
    #   geom_raster() +
    #   colorspace::scale_fill_binned_divergingx("spectral",
    #                                            mid = 0.5,
    #                                           na.value = "transparent",
    #                                           rev = F,
    #                                           limits = c(0,1),
    #                                           n.breaks = 11)

    # save result

    res_file <-
      str_glue(
        "era5_water-balance-perc-w{k}_bl-1991-2020_mon_{date_to_proc}.nc"
      )

    res_path <-
      str_glue("{dir_data}/{res_file}")

    rt_write_nc(
      s_perc,
      res_path,
      gatt_name = "source code",
      gatt_val = "https://github.com/carlosdobler/drought-monitor-forecast"
    )

    # upload to gcloud
    # str_glue("gsutil mv {res_path} {dir_gs}/water_balance_th_perc/") |>
    #   system(ignore.stdout = T, ignore.stderr = T)
    str_glue("gcloud storage mv {res_path} gs://drought-monitor/historical/") |>
      system(ignore.stdout = T, ignore.stderr = T)
  } # end of else (if wb quantile was missing)
} # end of window loop

fs::dir_delete(dir_data)
