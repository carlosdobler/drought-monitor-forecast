land_mask <- function(stars_obj) {
  # reference grid

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
    st_warp(stars_obj, method = "max", use_gdal = T) |>
    setNames("land")

  # format dimensions
  st_dimensions(land_r) <- st_dimensions(stars_obj)[1:2]

  return(land_r)
}


load_data <- function(
  dir_origin_cloud, # bucket dir where all vars are located
  dir_dest_local,
  date_vector
) {
  fs::dir_create(dir_dest_local)

  # download data

  ff <-
    rt_gs_list_files(dir_origin_cloud) |>
    str_subset(".nc") |>
    str_subset(str_flatten(
      seq(year(first(date_vector)), year(last(date_vector))),
      "|"
    ))

  ff <-
    rt_gs_download_files(ff, dir_dest_local)

  # load data

  ss <-
    ff |>
    future_map(\(f) {
      read_ncdf(f) |>
        suppressMessages() |>
        adrop()
    })

  # concatenate

  ss <-
    do.call(c, c(ss, along = "time")) |>
    st_set_dimensions("time", values = date_vector)

  # clean up

  fs::dir_delete(dir_dest_local)

  return(ss)
}


distr_params_apply <- function(x, ...) {
  if (any(is.na(x))) {
    params <-
      rep(NA, length(param_names)) |>
      set_names(param_names)
  } else if (length(unique(x)) == 1) {
    params <-
      rep(-9999, length(param_names)) |>
      set_names(param_names)
  } else {
    params <-
      distribution(samlmu(x))
  }

  return(params)
}


distr_params <- function(
  s,
  dir_tmp_local,
  dir_output_cloud,
  distribution,
  f_name_root,
  process
) {
  fs::dir_create(dir_tmp_local)

  param_names <-
    sample(10, 100, replace = T) |>
    samlmu() |>
    distribution() |>
    names()

  walk(seq(12), \(mon) {
    print(str_glue("fitting month {mon}"))

    if (process == "era") {
      r <-
        s |>
        filter(month(time) == mon) |>
        units::drop_units() |>

        st_apply(c(1, 2), distr_params_apply, FUTURE = T, .fname = "params") |>
        split("params")
    } else if (process == "nmme") {
      r <-
        s |>
        filter(month(time) == mon) |>
        units::drop_units() |>

        st_apply(
          c(1, 2, 3),
          distr_params_apply,
          FUTURE = T,
          .fname = "params"
        ) |>
        split("params")
    }

    # save results

    f <-
      str_glue(
        "{dir_tmp_local}/{f_name_root}_{str_pad(mon, 2, 'left', '0')}.nc"
      )

    rt_write_nc(
      r,
      f,
      gatt_name = "source code",
      gatt_val = "https://github.com/carlosdobler/drought/distribution_parameters"
    )

    # # move to cloud
    # str_glue("gcloud storage mv {f} {dir_output_cloud}") |>
    #   system(ignore.stdout = T, ignore.stderr = T)
  })

  # fs::dir_delete(dir_tmp_local)
}


rollsum <- function(s, k, suffix_var) {
  nn <- str_glue("{suffix_var}{k}")
  dates <- st_get_dimension_values(s, "time")
  un <- s |> pull() |> units::deparse_unit()

  s |>
    st_apply(
      c(1, 2),
      \(x) {
        slider::slide_dbl(x, sum, .before = k - 1, .complete = T)
      },
      .fname = "time"
    ) |>
    aperm(c(2, 3, 1)) |>
    st_set_dimensions("time", values = dates) |>
    setNames(nn) |>
    mutate(!!sym(nn) := units::set_units(!!sym(nn), !!un))
}
