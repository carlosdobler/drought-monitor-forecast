land_mask <- function(ref_grid) {
  # Generates a land mask based on the dimensions/resolution
  # of ref_grid. Excludes Antarctica.

  f_land <- "gs://clim_data_reg_useast1/misc_data/physical/ne_50m_land"

  str_glue("gcloud storage cp -r {f_land} {tempdir()}") |>
    system(ignore.stdout = T, ignore.stderr = T)

  land_pol <-
    str_glue("{tempdir()}/ne_50m_land") |>
    st_read(quiet = T)

  fs::dir_delete(str_glue("{tempdir()}/ne_50m_land"))

  # extract centroid coordinates
  centr <-
    land_pol |>
    st_centroid() |>
    st_coordinates() |>
    as_tibble()

  # filter out antarctica >
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
    st_warp(ref_grid, method = "max", use_gdal = T) |>
    setNames("land")

  land_r[land_r == 0] <- NA

  # format dimensions
  st_dimensions(land_r) <- st_dimensions(ref_grid)[1:2]

  return(land_r)
}


# *****

# load_data <- function(
#   dir_origin_cloud, # bucket dir where all vars are located
#   dir_dest_local,
#   date_vector
# ) {
#   # Downloads data corresponding to date_vector
#   # from dir_origin_cloud into dir_dest_local. Then
#   # loads it into memory

#   if (!fs::dir_exists(dir_dest_local)) {
#     fs::dir_create(dir_dest_local)
#   }

#   # download data

#   ff <-
#     rt_gs_list_files(dir_origin_cloud) |>
#     str_subset(".nc") |>
#     str_subset(
#       str_flatten(
#         seq(year(first(date_vector)), year(last(date_vector))),
#         "|"
#       )
#     )

#   ff <-
#     rt_gs_download_files(ff, dir_dest_local)

#   # load data
#   ss <-
#     ff |>
#     map(\(f) {
#       f |>
#         read_mdim() |>
#         adrop()
#     })

#   # concatenate

#   ss <-
#     do.call(c, c(ss, along = "time")) |>
#     st_set_dimensions("time", values = date_vector)

#   # clean up

#   fs::file_delete(ff)

#   return(ss)
# }

# *****

distr_params_apply <- function(x, param_names, distribution) {
  # Function to be used in distr_params
  # (inherits arguments from it)

  if (any(is.na(x))) {
    params <-
      rep(NA, length(param_names)) |>
      set_names(param_names)
  } else if (mean(x == 0) > 0.9) {
    params <-
      rep(-99999, length(param_names)) |>
      set_names(param_names)
  } else {
    params <-
      distribution(samlmu(x))
  }

  return(params)
}


# *****

# distr_params <- function(
#   s,
#   dir_tmp_local,
#   dir_output_cloud,
#   distribution,
#   f_name_root,
#   process
# ) {
#   # Fits a distribution, outputs its parameters

#   fs::dir_create(dir_tmp_local)

#   # get names from sim l-moments vector
#   param_names <-
#     names(distribution(c(1, 0.1, 0.1, 0.1)))

#   walk(seq(12), \(mon) {
#     print(str_glue("fitting month {mon} / 12"))

#     if (process == "era") {
#       #
#       r <-
#         s |>
#         filter(month(time) == mon) |>
#         units::drop_units() |>

#         st_apply(
#           c(1, 2),
#           distr_params_apply,
#           param_names = param_names,
#           distribution = distribution,
#           FUTURE = T,
#           .fname = "params"
#         ) |>
#         split("params")
#       #
#     } else if (process == "nmme") {
#       #
#       r <-
#         s |>
#         filter(month(time) == mon) |>
#         units::drop_units() |>

#         st_apply(
#           c(1, 2, 3), # pool all members to fit distr
#           distr_params_apply,
#           param_names = param_names,
#           distribution = distribution,
#           FUTURE = T,
#           .fname = "params"
#         ) |>
#         split("params")
#       #
#     }

#     # save results

#     f <-
#       str_glue(
#         "{dir_tmp_local}/{f_name_root}_{str_pad(mon, 2, 'left', '0')}.nc"
#       )

#     rt_write_nc(r, f)

#     # move to cloud
#     str_glue("gcloud storage mv {f} {dir_output_cloud}") |>
#       system(ignore.stdout = T, ignore.stderr = T)
#   })

#   fs::dir_delete(dir_tmp_local)
# }

rollsum <- function(s, k, suffix_var) {
  # Calculates a rolling sum of k width over s (stars obj);
  # names the variable with suffix_var

  nn <- str_glue("{suffix_var}{k}")
  dates <- st_get_dimension_values(s, "time")
  un <- s |> pull() |> units::deparse_unit()

  s |>
    st_apply(
      c(1, 2),
      \(x) {
        slider::slide_sum(x, before = k - 1, complete = T)
      },
      .fname = "time"
    ) |>
    aperm(c(2, 3, 1)) |>
    st_set_dimensions("time", values = dates) |>
    setNames(nn) |>
    mutate(!!sym(nn) := units::set_units(!!sym(nn), !!un))
}
