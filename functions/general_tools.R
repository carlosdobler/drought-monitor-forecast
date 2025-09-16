#' List files in a google bucket directory
#'
#' @description
#' Function to list all files in a google cloud bucket
#' directory
#'
#' @param dir google cloud bucket directory
#'
#' @export
rt_gs_list_files <- function(dir) {
  stringr::str_glue("gcloud storage ls {dir}") |>
    system(intern = T)
}


#' Download file(s) from a google bucket directory
#'
#' @description
#' Function to download one or multiple files from a google cloud bucket to a local
#' directory. When assigned to an object, that object will contain the updated file
#' name(s) (with path reflecting the destination directory). The function downloads
#' files in parallel by default using either mirai or future, depending on whether
#' daemons or a plan (e.g. multicore) have been called beforehand. If no daemons or
#' plan has been specified, downloads will be sequential. The option to force downloads
#' to be sequential is also available.
#'
#' @param f file(s) in the bucket to download (with full path; e.g. gs://...)
#' @param dest local destination directory
#' @param quiet (boolean) if FALSE (default), omits printing whether files are being downloaded in parallel
#' @param parallel (boolean) TRUE downloads in parallel with mirai (if dameons have already been called) or
#' with future (if a plan has been specified). FALSE forces sequential downloads.
#' @param gsutil (boolean) if FALSE (default), downloads with "gcloud storage" instead of "gsutil"
#'
#' @export
rt_gs_download_files <- function(f, dest, quiet = F, parallel = T, gsutil = F) {
  # create directory "dest" if inexistent
  if (!fs::dir_exists(dest)) {
    fs::dir_create(dest)
  }

  if (any(stringr::str_sub(f, end = 2) != "gs")) {
    # if path is wrong (not in google cloud)
    print(stringr::str_glue("ERROR: not a google cloud directory"))
    #
  } else {
    # identify the parallel engine to use
    if (parallel & mirai::daemons()$connections > 0) {
      parallel_ <- "m"
    } else if (parallel & !is(future::plan(), "sequential")) {
      parallel_ <- "f"
    } else {
      parallel_ <- "none"
    }

    # check whether gcloud or gsutil utility will be used
    if (gsutil) {
      cmd <- "gsutil"
    } else {
      cmd <- "gcloud storage"
    }

    # download
    if (parallel_ == "none") {
      if (!quiet) {
        message("   downloading sequentially...")
      }

      f |>
        purrr::walk(\(f_) {
          stringr::str_glue("{cmd} cp {f_} {dest}") |>
            system(ignore.stdout = T, ignore.stderr = T)
        })
      #
    } else if (parallel_ == "m") {
      if (!quiet) {
        message("   downloading in parallel (mirai)...")
      }

      f |>
        purrr::walk(
          purrr::in_parallel(
            \(f_) {
              stringr::str_glue("{cmd} cp {f_} {dest}") |>
                system(ignore.stdout = T, ignore.stderr = T)
            },
            cmd = cmd,
            dest = dest
          )
        )
    } else if (parallel_ == "f") {
      if (!quiet) {
        message("   downloading in parallel (future)...")
      }

      f |>
        furrr::future_walk(\(f_) {
          stringr::str_glue("{cmd} cp {f_} {dest}") |>
            system(ignore.stdout = T, ignore.stderr = T)
        })
    }

    # update names
    updated <-
      stringr::str_glue("{dest}/{fs::path_file(f)}")

    return(updated)
  }
}


#' Save stars objects into NetCDFs
#'
#' @description
#' Function to write stars objects as NetCDFs. It can save
#' objects with multiple variables. If variables have units,
#' they will be saved.
#'
#' @param stars_obj a stars object with either two or more
#' dimensions; lon (x) and lat (y) should be the first two, in that order.
#' @param filename where should the file be saved?
#' @param calendar one of "360_day", "gregorian", or "noleap". If not
#' provided, it will guess from the stars obj (needs February months to work)
#' @param gatt_name global attribute name
#' @param gatt_val global attribute value (or text)
#'
#' @export
rt_write_nc <- function(stars_obj, filename, calendar = NA, gatt_name = NA, gatt_val = NA) {
  dims <- vector("list", length(dim(stars_obj)))
  names(dims) <- names(dim(stars_obj))

  dims[[1]] <-
    ncdf4::ncdim_def(
      name = names(dims)[1],
      units = "degrees_east",
      vals = stars_obj |> stars::st_get_dimension_values(1)
    )

  dims[[2]] <-
    ncdf4::ncdim_def(
      name = names(dims)[2],
      units = "degrees_north",
      vals = stars_obj |> stars::st_get_dimension_values(2)
    )

  for (dim_i in seq_along(dims) |> utils::tail(-2)) {
    dim_vals <-
      stars_obj |>
      stars::st_get_dimension_values(dim_i)

    if (
      methods::is(dim_vals, "Date") |
        methods::is(dim_vals, "POSIXct") |
        methods::is(dim_vals, "PCICt")
    ) {
      time_vector_str <-
        dim_vals |>
        as.character() |>
        stringr::str_sub(end = 10)

      if (all(diff(dim_vals) == 1)) {
        if (!is.na(calendar)) {
          cal_spec <- calendar
        } else {
          feb <-
            time_vector_str[stringr::str_sub(time_vector_str, 6, 7) == "02"]

          if (length(feb) > 1) {
            max_feb <-
              feb |>
              stringr::str_sub(9, 10) |>
              as.numeric() |>
              max()

            cal_spec <-
              dplyr::case_when(
                max_feb == 30 ~ "360_day",
                max_feb == 29 ~ "gregorian",
                max_feb == 28 ~ "noleap"
              )
          } else {
            message("ERROR: no multiple Februaries: cannot guess calendar")
          }
        }

        time_vector <-
          PCICt::as.PCICt(time_vector_str, cal = cal_spec)
      } else {
        time_vector <-
          PCICt::as.PCICt(time_vector_str, cal = "gregorian")
      }

      cal <-
        dplyr::case_when(
          attributes(time_vector)$cal == "360" ~ "360_day",
          attributes(time_vector)$cal == "365" ~ "365_day",
          attributes(time_vector)$cal == "proleptic_gregorian" ~ "gregorian"
        )

      dims[[dim_i]] <-
        ncdf4::ncdim_def(
          name = names(dims)[dim_i],
          units = "days since 1970-01-01",
          vals = as.numeric(time_vector) / 86400,
          calendar = cal
        )
    } else {
      dims[[dim_i]] <-
        ncdf4::ncdim_def(name = names(dims)[dim_i], units = "", vals = dim_vals)
    }
  }

  var_names <- names(stars_obj)

  var_units <-
    purrr::map_chr(seq_along(var_names), \(x) {
      un <- try(
        stars_obj |>
          dplyr::select(dplyr::all_of(x)) |>
          dplyr::pull() |>
          units::deparse_unit(),
        silent = T
      )

      if (class(un) == "try-error") {
        un <- ""
      }
      return(un)
    })

  varis <-
    purrr::map2(var_names, var_units, ~ ncdf4::ncvar_def(name = .x, units = .y, dim = dims))

  ncnew <-
    ncdf4::nc_create(filename = filename, vars = varis, force_v4 = TRUE)

  if (!is.na(gatt_name)) {
    ncdf4::ncatt_put(ncnew, varid = 0, attname = gatt_name, attval = gatt_val)
  }

  purrr::walk(
    seq_along(var_names),
    ~ ncdf4::ncvar_put(
      nc = ncnew,
      varid = varis[[.x]],
      vals = stars_obj |> dplyr::select(dplyr::all_of(.x)) |> dplyr::pull()
    )
  )

  ncdf4::nc_close(ncnew)
}


#' Get cell position (index) from coordinates
#'
#' @description
#'  Function to obtain the position of cells in a grid
#'  given a set of coordinates. If four coordinates
#'  are provided, then the functions outputs start positions,
#'  end positions, and counts.
#'
#' @export
rt_from_coord_to_ind <- function(stars_obj, xmin, ymin, xmax = NA, ymax = NA) {
  if (is.na(xmax) & is.na(ymax)) {
    coords <-
      purrr::map2(c(xmin, ymin), c(1, 2), \(coord, dim_id) {
        s <-
          stars_obj |>
          stars::st_get_dimension_values(dim_id)

        which.min(abs(s - coord))
      }) |>
      purrr::set_names(c("x", "y"))

    r <-
      list(x = coords$x, y = coords$y)
  } else {
    coords <-
      purrr::map2(list(c(xmin, xmax), c(ymin, ymax)), c(1, 2), \(coords, dim_id) {
        s <-
          stars_obj |>
          stars::st_get_dimension_values(dim_id)

        purrr::map(coords, \(x) which.min(abs(s - x)))
      }) |>
      unlist(recursive = F) |>
      purrr::set_names(c("xmin", "xmax", "ymin", "ymax"))

    if (coords$ymax > coords$ymin) {
      r <-
        list(
          x_start = coords$xmin,
          y_start = coords$ymin,
          x_end = coords$xmax,
          y_end = coords$ymax,
          x_count = coords$xmax - coords$xmin + 1,
          y_count = coords$ymax - coords$ymin + 1
        )
    } else {
      r <-
        list(
          x_start = coords$xmin,
          y_start = coords$ymax,
          x_end = coords$xmax,
          y_end = coords$ymin,
          x_count = coords$xmax - coords$xmin + 1,
          y_count = coords$ymin - coords$ymax + 1
        )
    }
  }

  return(r)
}
