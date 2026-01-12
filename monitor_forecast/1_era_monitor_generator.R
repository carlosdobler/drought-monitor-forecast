# SCRIPT TO CALCULATE DROUGHT (WB ANOMALY) WITH ERA5 FOR 1 MONTH
# BASED ON A MODIFIED SPEI METHODOLOGY
# USES THORNWHAITES FORMULATION TO CALCULATE PET

# extract what wb quantiles dates have already been processed
existing_dates_final_bucket <-
  rt_gs_list_files(str_glue("gs://drought-monitor/historical")) |>
  str_subset(".nc$") |>
  str_subset(str_glue("-w12_")) |>
  str_sub(-13, -4)

if (date_to_proc %in% existing_dates_final_bucket) {
  stop("THIS DATE HAS ALREADY BEEN PROCESSED!")
}

# vector of all dates necessary to calculate rolling sum
pre_dates <-
  seq(
    as_date(date_to_proc) - months(max(winds) - 1), # max integration window
    as_date(date_to_proc),
    by = "1 month"
  )


# CHECK AVAILABLE FILES

# existing wb files in bucket
existing_dates_bucket <-
  rt_gs_list_files(str_glue(
    "{dir_gs_era}/monthly_totals/water_balance_hamon/*.nc"
  )) |>
  suppressWarnings() |>
  str_sub(-13, -4)

dates_in_bucket <-
  pre_dates[pre_dates %in% existing_dates_bucket]

# ... as inputs to calculate wb (temperature and precip)
existing_dates_inputs <-
  c("total_precipitation", "2m_temperature") |>
  set_names() |>
  map(\(var) {
    rt_gs_list_files(str_glue("{dir_gs_era}/monthly_means/{var}")) |>
      str_sub(-13, -4)
  })

dates_inputs <-
  pre_dates[pre_dates %in% existing_dates_inputs[[1]]]


# DOWNLOAD SECTION

# if some wb dates are in the bucket, download
if (length(dates_in_bucket) > 0) {
  ff <-
    str_glue(
      "{dir_gs_era}/monthly_totals/water_balance_hamon/era5_water-balance-hamon_mon_{dates_in_bucket}.nc"
    )

  rt_gs_download_files(ff, dir_data, quiet = T) |>
    invisible()
}

# if there are wb dates that are not in the bucket,
# but are as inputs, download
if (any(!dates_inputs %in% dates_in_bucket)) {
  ff <-
    c("total_precipitation", "2m_temperature") |>
    map(\(var) {
      rt_gs_list_files(str_glue("{dir_gs_era}/monthly_means/{var}")) |>
        str_subset(
          str_flatten(
            str_glue("_{dates_inputs[!dates_inputs %in% dates_in_bucket]}.nc"),
            "|"
          )
        )
    })

  ff |>
    walk(rt_gs_download_files, dir_data, quiet = T)
}

# if there are dates that are not as inputs, download from cds
if (any(!pre_dates %in% dates_inputs)) {
  #
  missing_dates <- pre_dates[!pre_dates %in% dates_inputs]

  reticulate::py_require("cdsapi")
  cdsapi <- reticulate::import("cdsapi")

  missing_dates |>
    walk(\(d) {
      c("total_precipitation", "2m_temperature") |>
        walk(\(var) {
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
          str_glue("gcloud storage cp {dir_data}/{f} {dir_gs_era}/monthly_means/{var}/") |>
            system(ignore.stdout = T, ignore.stderr = T)
        })
    })
} else {
  missing_dates <- NULL
}


# IMPORT WB DATES IN DISK

f_wb <-
  fs::dir_ls(dir_data) |>
  str_subset("era5_water-balance-hamon_mon_")

if (length(f_wb) > 0) {
  #
  dates_wb <-
    f_wb |>
    str_sub(-13, -4)

  s_era_wb_disk <-
    f_wb |>
    map(read_mdim)

  s_era_wb_disk <-
    do.call(c, c(s_era_wb_disk, along = "time")) |>
    st_set_dimensions("time", values = as_date(dates_wb))
}

# CALCULATE MISSING WB DATES

dates_missing_wb <-
  c(dates_inputs[!dates_inputs %in% dates_in_bucket], missing_dates)

ss_inputs <-
  c("total_precipitation", "2m_temperature") |>
  map(\(var) {
    ss <-
      str_glue(
        "{dir_data}/era5_{str_replace_all(var, '_', '-')}_mon_{dates_missing_wb}.nc"
      ) |>
      map(read_mdim) |>
      map(adrop)

    return(ss)
  })

s_pet <-
  map2(dates_missing_wb, ss_inputs[[2]], \(d, s) {
    pet_calculator_hamon(d, s)
  })

s_pet <-
  do.call(c, c(s_pet, along = "time")) |>
  st_set_dimensions("time", values = dates_missing_wb)

# convert to mm/month
s_pet <-
  s_pet |>
  units::drop_units() |>
  mutate(
    pet = pet * conv, # convert to mm/month
    pet = units::set_units(pet, mm / month)
  )

s_pr <-
  do.call(c, c(ss_inputs[[1]], along = "time")) |>
  st_set_dimensions("time", values = dates_missing_wb) |>
  units::drop_units() |>
  mutate(
    tp = tp * 1000 * conv,
    tp = units::set_units(tp, mm / month)
  )

s_era_wb <-
  c(s_pr, s_pet) |>
  mutate(wb = tp - pet) |>
  select(wb)

# save wb to bucket
dates_missing_wb |>
  walk(\(d) {
    f <- str_glue("{dir_data}/era5_water-balance-hamon_mon_{d}.nc")

    s_era_wb |>
      filter(time == d) |>
      adrop() |>
      rt_write_nc(f)

    str_glue("gcloud storage mv {f} {dir_gs_era}/monthly_totals/water_balance_hamon/") |>
      system(ignore.stdout = T, ignore.stderr = T)
  })

# COMBINE WITH WB IN DISK

if (length(f_wb) > 0) {
  s_era_wb <-
    c(s_era_wb_disk, s_era_wb)
}


# CALCULATE PERCENTILES (QUANTILE)

# loop through integration windows
walk(winds, \(k) {
  message(str_glue("*** PROCESSING {date_to_proc} (window = {k}) ***"))

  pre_dates_k <- pre_dates |> tail(k)

  # aggregate (rollsum)
  s_wb_rolled <-
    s_era_wb |>
    filter(time %in% pre_dates_k) |>
    st_apply(c(1, 2), sum, .fname = str_glue("wb_rollsum{k}"))

  # import baseline distribution parameters
  f_distr <-
    str_glue(
      "era5_water-balance-hamon-rollsum{k}_mon_log-params_1991-2020_{str_pad(month(as_date(date_to_proc)), 2, 'left', '0')}.nc"
    )

  s_dist_params <-
    rt_gs_download_files(
      str_glue("{dir_gs_era}/climatologies_monthly_totals/{f_distr}"),
      dir_data,
      quiet = T
    ) |>
    read_mdim()

  # calculate percentile (quantile)

  s_perc <-
    c(s_wb_rolled, s_dist_params) |>
    merge() |>
    st_apply(
      c(1, 2),
      \(x) {
        if (is.na(x)[2]) {
          NA
        } else {
          lmom::cdfglo(x[1], c(x[2], x[3], x[4]))
        }
      },
      .fname = "perc"
    )

  # # plot to check with spei csic
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
      "{dir_data}/era5_water-balance-perc-w{k}_bl-1991-2020_mon_{date_to_proc}.nc"
    )

  rt_write_nc(
    s_perc,
    res_file,
    gatt_name = "source code",
    gatt_val = "https://github.com/carlosdobler/drought-monitor-forecast"
  )

  # upload to cloud
  str_glue("gcloud storage mv {res_file} gs://drought-monitor/historical/") |>
    system(ignore.stdout = T, ignore.stderr = T)
})


f_wb |>
  fs::file_delete()

c("total-precipitation", "2m-temperature") |>
  walk(\(v) {
    dir_data |>
      fs::dir_ls() |>
      str_subset(v) |>
      fs::file_delete()
  })
