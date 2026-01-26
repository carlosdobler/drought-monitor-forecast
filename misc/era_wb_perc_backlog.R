# SCRIPT TO CALCULATE WATER BALANCE PERCENTILES FOR HISTORICAL PERIOD
# PROCESSES ALL DATES FROM 1971 TO 2025-11-01
# BATCH PROCESSING VERSION OF 1_era_monitor_generator.R

library(tidyverse)
library(stars)
library(furrr)
source("monitor_forecast/setup.R")
source("functions/general_tools.R")
source("functions/functions_drought.R")

plan(multicore)

# conversion constant (days in month)
conv <-
  (units::set_units(1, month) / units::set_units(1, d)) |>
  units::drop_units()

# SETUP ----

# Define date range
start_date <- as_date("1971-01-01")
end_date <- as_date("2025-11-01")

# All dates to process
dates_to_process <- seq(start_date, end_date, by = "1 month")

# Integration windows
winds <- c(3, 12)
max_window <- max(winds)

# Directories
dir_gs_era <- "gs://clim_data_reg_useast1/era5"


# DOWNLOAD BASELINE DISTRIBUTION PARAMETERS ----

message("=== DOWNLOADING BASELINE DISTRIBUTION PARAMETERS ===")

# Download all monthly distribution parameters for each window
dist_params_list <- map(winds, \(k) {
  message(str_glue("  Window {k}: loading parameters"))

  # Download all 12 months
  files <- str_glue(
    "era5_water-balance-hamon-rollsum{k}_mon_log-params_1991-2020_{str_pad(1:12, 2, 'left', '0')}.nc"
  )

  rt_gs_download_files(
    str_glue("{dir_gs_era}/climatologies_monthly_totals/{files}"),
    dir_data,
    quiet = TRUE,
    update_only = T # REMOVE!
  )

  # Read all 12 months
  params <- map(1:12, \(m) {
    read_mdim(str_glue("{dir_data}/{files[m]}"))
  })

  # Return as named list (month number as name)
  set_names(params, as.character(1:12))
})

# Name by window size
dist_params_list <- set_names(dist_params_list, as.character(winds))


# INITIALIZE: Download first 12 months of data ----

message("=== INITIALIZING: Downloading first 12 months ===")

# Initial dates: 11 months before first date + first date = 12 months total
initial_dates <- seq(
  start_date - months(max_window - 1),
  start_date,
  by = "1 month"
)

message(str_glue("Initial window: {min(initial_dates)} to {max(initial_dates)}"))

# Download precipitation and temperature
walk(
  c("total_precipitation", "2m_temperature"),
  \(var) {
    ff <- str_glue(
      "{dir_gs_era}/monthly_means/{var}/era5_{str_replace_all(var, '_', '-')}_mon_{initial_dates}.nc"
    )

    rt_gs_download_files(ff, dir_data, quiet = TRUE) |>
      invisible()
  }
)

# Import precipitation
s_pr <-
  str_glue("{dir_data}/era5_total-precipitation_mon_{initial_dates}.nc") |>
  future_map(read_mdim) |>
  map(adrop)

s_pr <-
  do.call(c, c(s_pr, along = "time")) |>
  st_set_dimensions("time", values = initial_dates) |>
  units::drop_units() |>
  mutate(
    tp = tp * 1000 * conv,
    tp = units::set_units(tp, mm / month)
  )

# Import temperature and calculate PET
s_temp <-
  str_glue("{dir_data}/era5_2m-temperature_mon_{initial_dates}.nc") |>
  future_map(read_mdim) |>
  map(adrop)

s_pet <-
  map2(initial_dates, s_temp, \(d, s) {
    pet_calculator_hamon(d, s)
  })

s_pet <-
  do.call(c, c(s_pet, along = "time")) |>
  st_set_dimensions("time", values = initial_dates) |>
  units::drop_units() |>
  mutate(
    pet = pet * conv,
    pet = units::set_units(pet, mm / month)
  )

# Calculate water balance
s_era_wb <-
  c(s_pr, s_pet) |>
  mutate(wb = tp - pet) |>
  select(wb)

# save to bucket for future use
res_file <- str_glue(
  "{dir_data}/era5_water-balance-hamon_mon_{start_date}.nc"
)

rt_write_nc(
  s_era_wb |> slice(time, 12),
  res_file,
  gatt_name = "source code",
  gatt_val = "https://github.com/carlosdobler/drought-monitor-forecast"
)

str_glue("gcloud storage mv {res_file} {dir_gs_era}/monthly_totals/water_balance_hamon/") |>
  system(ignore.stdout = TRUE, ignore.stderr = TRUE)

# Current dates in memory
current_dates <- initial_dates


# SLIDING WINDOW PROCESSING ----

message("\n=== STARTING SLIDING WINDOW PROCESSING ===\n")


for (dtp in as.character(dates_to_process)) {
  date_to_proc <- as_date(dtp)

  message(str_glue("Processing {date_to_proc}"))

  # CALCULATE PERCENTILES FOR CURRENT DATE ----

  walk(winds, \(k) {
    # Get dates for this window
    pre_dates_k <- seq(
      date_to_proc - months(k - 1),
      date_to_proc,
      by = "1 month"
    )

    # Aggregate (rolling sum)
    s_wb_rolled <-
      s_era_wb |>
      filter(time %in% pre_dates_k) |>
      st_apply(c(1, 2), sum, .fname = str_glue("wb_rollsum{k}"))

    # Get distribution parameters for this month
    month_num <- as.character(month(date_to_proc))
    s_dist_params <- dist_params_list[[as.character(k)]][[month_num]]

    # Calculate percentile
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

    # Save result
    res_file <- str_glue(
      "{dir_data}/era5_water-balance-perc-w{k}_bl-1991-2020_mon_{date_to_proc}.nc"
    )

    rt_write_nc(
      s_perc,
      res_file,
      gatt_name = "source code",
      gatt_val = "https://github.com/carlosdobler/drought-monitor-forecast"
    )

    # Upload to cloud
    # str_glue("gcloud storage mv {res_file} gs://drought-monitor/historical/") |>
    #   system(ignore.stdout = TRUE, ignore.stderr = TRUE)
    str_glue(
      "gcloud storage mv {res_file} {dir_gs_era}/monthly_totals/water_balance_hamon_perc/"
    ) |>
      system(ignore.stdout = TRUE, ignore.stderr = TRUE)
  })

  # SLIDE WINDOW: Remove oldest month, add new month ----

  # Only slide if not the last date
  if (date_to_proc < max(dates_to_process)) {
    next_date <- date_to_proc + months(1)
    oldest_date <- current_dates[1]

    message(str_glue("  Sliding window: removing {oldest_date}, adding {next_date}"))

    # Download new month's data
    walk(
      c("total_precipitation", "2m_temperature"),
      \(var) {
        ff <- str_glue(
          "{dir_gs_era}/monthly_means/{var}/era5_{str_replace_all(var, '_', '-')}_mon_{next_date}.nc"
        )

        rt_gs_download_files(ff, dir_data, quiet = TRUE) |>
          invisible()
      }
    )

    # Read new precipitation
    s_pr_new <-
      str_glue("{dir_data}/era5_total-precipitation_mon_{next_date}.nc") |>
      read_mdim() |>
      adrop() |>
      units::drop_units() |>
      mutate(
        tp = tp * 1000 * conv,
        tp = units::set_units(tp, mm / month)
      )

    # Add time dimension by wrapping in list and concatenating
    s_pr_new <- do.call(c, c(list(s_pr_new), along = "time")) |>
      st_set_dimensions("time", values = next_date)

    # Read new temperature and calculate PET
    s_temp_new <-
      str_glue("{dir_data}/era5_2m-temperature_mon_{next_date}.nc") |>
      read_mdim() |>
      adrop()

    s_pet_new <-
      pet_calculator_hamon(next_date, s_temp_new) |>
      units::drop_units() |>
      mutate(
        pet = pet * conv,
        pet = units::set_units(pet, mm / month)
      )

    # Add time dimension
    s_pet_new <- do.call(c, c(list(s_pet_new), along = "time")) |>
      st_set_dimensions("time", values = next_date)

    # Calculate new water balance
    s_wb_new <-
      c(s_pr_new, s_pet_new) |>
      mutate(wb = tp - pet) |>
      select(wb)

    # save to bucket for future use
    res_file <- str_glue(
      "{dir_data}/era5_water-balance-hamon_mon_{next_date}.nc"
    )

    rt_write_nc(
      s_wb_new |> adrop(),
      res_file,
      gatt_name = "source code",
      gatt_val = "https://github.com/carlosdobler/drought-monitor-forecast"
    )

    str_glue("gcloud storage mv {res_file} {dir_gs_era}/monthly_totals/water_balance_hamon/") |>
      system(ignore.stdout = TRUE, ignore.stderr = TRUE)

    # Remove oldest month from water balance and add new month
    s_era_wb <-
      s_era_wb |>
      filter(time != oldest_date) |>
      c(s_wb_new, along = "time")

    # Update current dates
    current_dates <- c(current_dates[-1], next_date)

    # Delete old files
    walk(
      c("total-precipitation", "2m-temperature"),
      \(v) {
        dir_data |>
          fs::dir_ls() |>
          str_subset(v) |>
          str_subset(str_replace_all(as.character(oldest_date), "-", "")) |>
          fs::file_delete()
      }
    )
  }
}

message("\n=== BACKLOG PROCESSING COMPLETE ===")


# CLEANUP: Delete distribution parameter files ----

message("\n=== CLEANING UP DISTRIBUTION PARAMETER FILES ===")

walk(winds, \(k) {
  files <- str_glue(
    "{dir_data}/era5_water-balance-hamon-rollsum{k}_mon_log-params_1991-2020_{str_pad(1:12, 2, 'left', '0')}.nc"
  )
  fs::file_delete(files)
})

message("=== DONE ===")
