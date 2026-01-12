# - SCRIPT TO OBTAIN DISTRIBUTION PARAMETERS OF BASELINE (1991-2020) ERA5 DATA
# - PARAMETERS OF TEMPERATURE AND PRECIP ARE USED TO BIAS-CORRECT NMME DATA
# - PARAMETERS OF WB (VARIABLE INTEGRATION WINDOWS) ARE USED TO CALCULATE WB ANOMALIES (PERCENTILES)

# SET UP ----------------------------------------------------------------------

library(tidyverse)
library(stars)
library(furrr)
library(lmom)

plan(multicore, workers = parallelly::availableCores() - 1)

# set data directory
source("distribution_parameters/setup.R")

# load general functions
source("functions/general_tools.R")
source("functions/tile.R")

# load general drought functions
source("functions/functions_drought.R")

# load distribution functions
source("functions/functions_distributions.R")


# PROCESS ---------------------------------------------------------------------

# land mask
mask <-
  st_bbox(c(xmin = -0.125, ymin = -90.125, xmax = 359.875, ymax = 90.125), crs = 4326) |>
  st_as_stars(dx = 0.25) |>
  st_set_dimensions(c(1, 2), names = c("longitude", "latitude")) |>
  land_mask() |>
  suppressWarnings()


# dates to process
# add 1 year at the beginning for rolling windows
date_vector_full <-
  seq(as_date("1990-01-01"), as_date("2020-12-01"), by = "1 month")

conv <-
  (units::set_units(1, month) / units::set_units(1, d)) |>
  units::drop_units()


df_tiles <-
  rt_tile_table(mask, 300, mask)


# ******

# DOWNLOAD TAS AND PRECIP DATA

fff <-
  c(
    tas = "gs://clim_data_reg_useast1/era5/monthly_means/2m_temperature/",
    precip = "gs://clim_data_reg_useast1/era5/monthly_means/total_precipitation/"
  ) |>
  map(\(dir) {
    rt_gs_list_files(dir) |>
      str_subset(
        str_flatten(seq(1990, 2020), "|")
      )
  })

fff <-
  fff |>
  map(\(ff) {
    ff |>
      rt_gs_download_files(dir_data, quiet = T)
  })

# some files were saved with time dimension (1 timestep)
# this messes up following steps
# identify which ones have 2 vs 3 dims

dims_length <-
  fff |>
  map(\(ff) {
    ff |>
      map_int(\(f) {
        f |>
          read_mdim(proxy = T) |>
          dim() |>
          length()
      })
  })


# BY TILES

df_tiles_m <-
  df_tiles |>
  filter(mask == T)

for (i in seq(nrow(df_tiles_m))) {
  #
  message(" ")
  message(str_glue("processing tile {i} / {nrow(df_tiles_m)}"))

  # LOAD DATA

  df_matrix <-
    cbind(
      c((df_tiles_m$start_x[i] - 1), (df_tiles_m$start_y[i] - 1), 0),
      c(df_tiles_m$count_x[i], df_tiles_m$count_y[i], NA)
    )

  ss <-
    map2(fff, dims_length, \(ff, dim_length) {
      #
      s_tile <-
        future_map2(ff, dim_length, \(f, d) {
          #
          if (d == 2) {
            df_matrix <- df_matrix[1:2, ]
          }

          read_mdim(
            f,
            offset = df_matrix[, 1],
            count = df_matrix[, 2]
          ) |>
            adrop()
        })

      s_tile <-
        do.call(c, c(s_tile, along = "time")) |>
        st_set_dimensions("time", values = date_vector_full)

      return(s_tile)
    })

  # MASK LAND
  mask_sub <-
    mask[,
      df_tiles_m$start_x[i]:df_tiles_m$end_x[i],
      df_tiles_m$start_y[i]:df_tiles_m$end_y[i]
    ]

  ss <-
    ss |>
    map(\(s) {
      s[is.na(mask_sub)] <- NA
      return(s)
    })

  # convert precip to mm/month
  ss[[2]] <-
    ss[[2]] |>
    units::drop_units() |>
    mutate(
      tp = tp * 1000 * conv,
      tp = units::set_units(tp, mm / month)
    )

  # FIT DISTRIBUTIONS

  pwalk(
    list(ss, list(pelgno, pelgam), names(ss)),
    \(s, distribution, variable) {
      #
      message(str_glue("fitting {variable}"))

      # get names from simulated l-moments vector
      param_names <-
        names(distribution(c(1, 0.1, 0.1, 0.1)))

      # split s by calendar months
      s_mon <-
        map(set_names(seq(12)), \(mon) {
          s |>
            filter(year(time) >= 1991, month(time) == mon) |>
            units::drop_units()
        })

      future_iwalk(s_mon, \(s, mon) {
        #
        r <-
          s |>
          st_apply(
            c(1, 2),
            distr_params_apply,
            param_names = param_names,
            distribution = distribution,
            .fname = "params"
          ) |>
          split("params")
        #

        rt_write_nc(
          r,
          str_glue(
            "{dir_data}/distr_tile-{df_tiles_m$tile_id[i]}_mon-{str_pad(mon, 2, 'left', '0')}_var-{v}.nc",
            v = variable
          )
        )
      })
    }
  )

  # CALCULATE PET
  s_pet <-
    date_vector_full |>
    map(\(d) {
      pet_calculator_hamon(
        d,
        ss[[1]] |>
          filter(time == d) |>
          adrop()
      )
    })

  s_pet <-
    do.call(c, c(s_pet, along = "time")) |>
    st_set_dimensions("time", values = date_vector_full)

  # convert to mm/month
  s_pet <-
    s_pet |>
    units::drop_units() |>
    mutate(
      pet = pet * conv, # convert to mm/month
      pet = units::set_units(pet, mm / month)
    )

  # CALCULATE WB
  s_wb <-
    c(ss[[2]], s_pet) |>
    mutate(wb = tp - pet) |>
    select(wb)

  # INTEGRATION WINDOWS
  message(str_glue("roll-summing"))

  ss_wb_rolled <-
    c(3, 12) |>
    set_names() |>
    map(\(k_) {
      rollsum(s_wb, k_, "wb_rollsum")
    })

  # FIT DISTRIBUTION
  iwalk(ss_wb_rolled, \(s, k_) {
    #
    message(str_glue("fitting wb window {k_}"))

    # get names from simulated l-moments vector
    param_names <-
      names(pelglo(c(1, 0.1, 0.1, 0.1)))

    # split s by calendar months
    s_mon <-
      map(set_names(seq(12)), \(mon) {
        s |>
          filter(year(time) >= 1991, month(time) == mon) |>
          units::drop_units()
      })

    future_iwalk(s_mon, \(s, mon) {
      #
      r <-
        s |>
        st_apply(
          c(1, 2),
          distr_params_apply,
          param_names = param_names,
          distribution = pelglo,
          .fname = "params"
        ) |>
        split("params")
      #

      rt_write_nc(
        r,
        str_glue(
          "{dir_data}/distr_tile-{df_tiles_m$tile_id[i]}_mon-{str_pad(mon, 2, 'left', '0')}_var-wb-{k_}.nc",
          k_ = k_
        )
      )
    })
  })
}

# MOSAIC ALL

list(
  c(
    "tas",
    "precip",
    "wb-3",
    "wb-12"
  ),
  c(
    "2m-temperature",
    "total-precipitation",
    "water-balance-hamon-rollsum3",
    "water-balance-hamon-rollsum12"
  ),
  c(
    "norm",
    "gamma",
    "log",
    "log"
  ),
  c(
    "climatologies",
    "climatologies_monthly_totals",
    "climatologies_monthly_totals",
    "climatologies_monthly_totals"
  )
) |>
  pwalk(\(variable, long_var, dist, dir) {
    #
    message(str_glue("mosaicking {variable}"))

    ff <-
      dir_data |>
      fs::dir_ls(regexp = str_glue("_var-{variable}"))

    walk(str_pad(seq(12), 2, "left", "0"), \(mon) {
      #
      message(str_glue("   {mon} / 12"))

      ss_tiles <-
        ff |>
        str_subset(str_glue("mon-{mon}")) |>
        map(read_mdim)

      mos <-
        rt_mosaic(ss_tiles)

      f_res <- str_glue("{dir_data}/era5_{long_var}_mon_{dist}-params_1991-2020_{mon}.nc")

      rt_write_nc(
        mos,
        f_res
      )

      str_glue("gcloud storage mv {f_res} gs://clim_data_reg_useast1/era5/{dir}") |>
        system(ignore.stdout = T, ignore.stderr = T)
    })

    fs::file_delete(ff)
  })

fff |>
  walk(fs::file_delete)
