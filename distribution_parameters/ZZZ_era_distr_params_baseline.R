# NOT NECESSARY ANYMORE!

# SCRIPT TO CALCULATE THE BASELINE (1991-2020)
# DISTRIBUTION PARAMETERS OF PRECIP, TAS, AND X-MONTH
# ROLLING SUM OF WATER BALANCE WITH ERA5 DATA

# WATER BALANCE PARAMETERS ARE USED TO GET MONITOR
# PERCENTILES. PRECIP AND TAS PARAMETERS ARE USED TO
# BIAS-CORRECT NMME DATA

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
plan(multicore)


# load special functions
source(
  "https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R"
)
source("monitor_forecast/functions.R")


# temporary directory to save files
dir_tmp <- "/mnt/pers_disk/tmp"
if (fs::dir_exists(dir_tmp)) {
  fs::dir_delete(dir_tmp)
}
fs::dir_create(dir_tmp)


# root cloud path
dir_gs <- "gs://clim_data_reg_useast1/era5"


# date vector
date_vector <-
  # start in 1990 to accomodate rolling sums
  seq(as_date("1990-01-01"), as_date("2020-12-01"), by = "1 month")


# 1. DOWNLOAD DATA ----

vars <- c("total_precipitation", "2m_temperature")

ff <-
  map(vars, \(var) {
    ff <-
      rt_gs_list_files(str_glue("{dir_gs}/monthly_means/{var}")) |>
      str_subset(str_flatten(
        seq(year(first(date_vector)), year(last(date_vector))),
        "|"
      ))

    ff <-
      rt_gs_download_files(ff, dir_tmp)
    # ff <-
    #   ff |>
    #   map_chr(\(f) str_glue("{dir_tmp}/{fs::path_file(f)}"))

    return(ff)
  })


# 2. HEAT INDEX VARIABLES ----

heat_vars <- heat_index_var_generator()


# 3A. CALCULATE WATER BALANCE ----

ss <-
  map(date_vector, \(d) {
    s_tas <-
      read_ncdf(str_glue(
        "{dir_tmp}/era5_2m-temperature_mon_{as_date(d)}.nc"
      )) |>
      suppressMessages() |>
      adrop()

    s_pr <-
      read_ncdf(str_glue(
        "{dir_tmp}/era5_total-precipitation_mon_{as_date(d)}.nc"
      )) |>
      suppressMessages() |>
      adrop()

    s_wb <-
      wb_calculator(
        d,
        s_tas |>
          setNames("tas") |>
          mutate(tas = tas |> units::set_units(degC)),
        s_pr |>
          setNames("pr"),
        heat_vars
      )

    list(wb = s_wb, tas = s_tas, pr = s_pr)
  })


ss <-
  ss |>
  transpose() |>
  map(\(x) do.call(c, c(x, along = "time"))) |>
  map(\(x) x |> st_set_dimensions("time", values = date_vector))


# 4. LAND MASK ----

f_land <- "gs://clim_data_reg_useast1/misc_data/physical/ne_50m_land"

str_glue("gcloud storage cp -r {f_land} {tempdir()}") |> system()


land_pol <-
  str_glue("{tempdir()}/ne_50m_land") |>
  st_read()

fs::dir_delete(str_glue("{tempdir()}/ne_50m_land"))

# extract centroid coordinates
centr <-
  land_pol |>
  st_centroid() |>
  st_coordinates() |>
  as_tibble()

# filter out antarctica
# (centroid < -60 latitude)
# rasterize
land_r <-
  land_pol |>
  mutate(Y = centr$Y) |>
  filter(Y > -60) |>
  mutate(a = 1) |>
  select(a) |>
  st_rasterize(st_as_stars(st_bbox(), dx = 0.1, values = 0))

# warp raster
land_r <-
  land_r |>
  st_warp(ss$wb |> slice(time, 1), method = "max", use_gdal = T) |>
  setNames("land")

# format dimensions
st_dimensions(land_r) <- st_dimensions(ss$wb)[1:2]

# mask out oceans in all data
ss <-
  ss |>
  map(\(s) {
    s[land_r == 0] <- NA
    return(s)
  })


# 5. FIT DISTRIBUTIONS -----

# water balance: generalized logistic (glo): see here: https://github.com/sbegueria/SPEI/blob/master/R/spei.R#L681-L698 (also line 717)
# precipitation: gamma
# temperature: normal

# final units:
# water balance: m
# precipitation: m
# temperature: K

ss |>
  iwalk(\(s, i) {
    # loop through months
    walk(seq(12), \(mon) {
      print(str_glue("var: {i}  |  month: {mon}"))

      # if variable = wb, for each year, subset month mon plus k-1 prior,
      # then aggregate (rollsum k-month window), and then concatenate
      if (i == "wb") {
        m <-
          # loop through years (leave 1990 out)
          map(
            seq(year(first(date_vector)) + 1, year(last(date_vector))),
            \(yr) {
              d <- as_date(str_glue("{yr}-{mon}-01"))

              # subset 1 month and k prior > aggregate
              nn <- str_glue("wb_rollsum{k}")
              s |>
                filter(time %in% seq(d - months(k - 1), d, by = "1 month")) |>
                st_apply(c(1, 2), sum, .fname = nn, FUTURE = T) |>
                mutate(!!sym(nn) := units::set_units(!!sym(nn), m))
            }
          )

        # concatenate
        m <-
          do.call(c, c(m, along = "yr"))

        # if variable = tas or precip, subset month and leave 1990 out
      } else {
        m <-
          s |>
          filter(
            month(time) == mon,
            year(time) %in%
              seq(year(first(date_vector)) + 1, year(last(date_vector)))
          )
      }

      # fit distributions
      r <-
        m |>
        units::drop_units() |>
        st_apply(
          c(1, 2),
          \(x) {
            if (i == "wb") {
              if (any(is.na(x))) {
                params <- c(xi = NA, alpha = NA, k = NA)
              } else {
                lmoms <- lmom::samlmu(x)
                params <- lmom::pelglo(lmoms)
              }
            } else if (i == "tas") {
              if (any(is.na(x))) {
                params <- c(xi = NA, alpha = NA, k = NA)
              } else {
                lmoms <- lmom::samlmu(x)
                params <- lmom::pelgno(lmoms)
              }
            } else if (i == "pr") {
              if (any(is.na(x))) {
                params <- c(alpha = NA, beta = NA)
              } else if (all(x == 0)) {
                params <- c(alpha = -9999, beta = -9999)
              } else {
                x[x == 0] <- 1e-10
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

      # save results

      f <-
        case_when(
          i == "wb" ~
            str_glue(
              "{dir_tmp}/era5_water-balance-th-rollsum{k}_mon_log-params_1991-2020_{str_pad(mon, 2, 'left', '0')}.nc"
            ),
          i == "tas" ~
            str_glue(
              "{dir_tmp}/era5_2m-temperature_mon_norm-params_1991-2020_{str_pad(mon, 2, 'left', '0')}.nc"
            ),
          i == "pr" ~
            str_glue(
              "{dir_tmp}/era5_total-precipitation_mon_gamma-params_1991-2020_{str_pad(mon, 2, 'left', '0')}.nc"
            )
        )

      rt_write_nc(r, f)

      # move to cloud
      str_glue(
        "gcloud storage mv {f} gs://clim_data_reg_useast1/era5/climatologies/"
      ) |>
        system()
    })
  })


# clean up
fs::dir_delete(dir_tmp)
