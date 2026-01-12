# DOWNLOAD ALL NMME DATES NECESSARY TO FIT BASELINE DISTRIBUTIONS

library(tidyverse)
library(stars)
library(furrr)

plan(multicore, workers = parallelly::availableCores() - 1)

source("distribution_parameters/setup.R")
source("functions/general_tools.R")
source("functions/functions_drought.R")
source("monitor_forecast/nmme_sources_df.R")

vars <- c("tref", "prec")
vars_long <- c("average-temperature", "precipitation")


# PROCESS ---------------------------------------------------------------------

for (i in seq_along(vars)) {
  var <- vars[i]
  var_l <- vars_long[i]

  for (mod in df_sources$model) {
    message(str_glue("variable: {var}  |  model: {mod}"))

    root_gs <-
      str_glue("gs://clim_data_reg_useast1/nmme/monthly/{mod}/{str_replace(var_l, '-', '_')}/")

    walk(seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month"), \(d) {
      #
      message(str_glue("{var} - {d}"))

      url <-
        nmme_url_generator(mod, d, var, df = df_sources)

      f <-
        str_glue("{dir_data}/nmme_{mod}_{var_l}_mon_ic-{as_date(d)}_leads-7_pre.nc")

      a <- "a" # empty vector
      class(a) <- "try-error" # assign error class

      while (class(a) == "try-error") {
        a <-
          try(
            download.file(url, f, method = "wget", quiet = T)
          )

        if (class(a) == "try-error") {
          print(stringr::str_glue("      download failed - waiting to retry..."))
          Sys.sleep(3)
        }
      }

      s <-
        nmme_formatter(f, var)

      f_formatted <-
        f |>
        str_replace("_pre.nc$", ".nc")

      rt_write_nc(s, f_formatted)

      str_glue(
        "gcloud storage mv {f_formatted} {root_gs}"
      ) |>
        system(ignore.stdout = T, ignore.stderr = T)

      fs::file_delete(f)
    })
  }
}
