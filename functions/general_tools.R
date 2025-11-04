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
  # build command

  stringr::str_glue("gcloud storage ls {dir}") |>
    # run command
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
rt_gs_download_files <- function(
  f,
  dest,
  quiet = F,
  parallel = T,
  gsutil = F,
  update_only = F
) {
  #
  if (!update_only) {
    # create destination directory if it does not exist
    if (!fs::dir_exists(dest)) {
      fs::dir_create(dest)
    }

    # check whether the path is a google cloud one
    if (any(stringr::str_sub(f, end = 2) != "gs")) {
      # if path is wrong (not in google cloud), print error message
      print(stringr::str_glue("ERROR: not a google cloud directory"))
      #
    } else {
      # if path is correct, proceed
      # identify the parallel engine to use ("m" for mirai, "f" for future)
      parallel_ <- "none"
      if (parallel) {
        if (
          requireNamespace("mirai", quietly = TRUE) &&
            mirai::status()$connections > 0
        ) {
          parallel_ <- "m"
        } else if (
          requireNamespace("furrr", quietly = TRUE) &&
            !is(future::plan(), "sequential")
        ) {
          parallel_ <- "f"
        } else {
          parallel_ <- "none"
        }
      }
      # check whether gcloud or gsutil utility will be used
      if (gsutil) {
        cmd <- "gsutil"
      } else {
        cmd <- "gcloud storage"
      }

      # download files
      if (parallel_ == "none") {
        # sequential download
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
        # parallel download with mirai
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
        # parallel download with future
        if (!quiet) {
          message("   downloading in parallel (future)...")
        }

        f |>
          furrr::future_walk(\(f_) {
            stringr::str_glue("{cmd} cp {f_} {dest}", cmd = cmd, dest = dest) |>
              system(ignore.stdout = T, ignore.stderr = T)
          })
      }
    }
  }

  # update file names to reflect their new local path
  updated <-
    stringr::str_glue("{dest}/{fs::path_file(f)}")

  return(updated)
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
rt_write_nc <- function(
  stars_obj,
  filename,
  calendar = NA,
  gatt_name = NA,
  gatt_val = NA
) {
  # create a list to store dimensions
  dims <- vector("list", length(dim(stars_obj)))
  names(dims) <- names(dim(stars_obj))

  # define lon dimension
  dims[[1]] <-
    ncdf4::ncdim_def(
      name = names(dims)[1],
      units = "degrees_east",
      vals = stars_obj |> stars::st_get_dimension_values(1)
    )

  # define lat dimension
  dims[[2]] <-
    ncdf4::ncdim_def(
      name = names(dims)[2],
      units = "degrees_north",
      vals = stars_obj |> stars::st_get_dimension_values(2)
    )

  # define other dimensions
  for (dim_i in seq_along(dims) |> utils::tail(-2)) {
    dim_vals <-
      stars_obj |>
      stars::st_get_dimension_values(dim_i)

    # if dimension is time, handle calendars
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
        # if the time difference is 1 day, then the calendar
        # can be guessed if not provided
        if (!is.na(calendar)) {
          cal_spec <- calendar
        } else {
          # get february days to guess calendar
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

        # create PCICt vector
        time_vector <-
          PCICt::as.PCICt(time_vector_str, cal = cal_spec)
      } else {
        # if time difference is not 1 day, assume gregorian
        time_vector <-
          PCICt::as.PCICt(time_vector_str, cal = "gregorian")
      }

      # get calendar name from PCICt object
      cal <-
        dplyr::case_when(
          attributes(time_vector)$cal == "360" ~ "360_day",
          attributes(time_vector)$cal == "365" ~ "365_day",
          attributes(time_vector)$cal == "proleptic_gregorian" ~ "gregorian"
        )

      # define time dimension
      dims[[dim_i]] <-
        ncdf4::ncdim_def(
          name = names(dims)[dim_i],
          units = "days since 1970-01-01",
          vals = as.numeric(time_vector) / 86400,
          calendar = cal
        )
    } else {
      # if dimension is not time, create a simple dimension
      dims[[dim_i]] <-
        ncdf4::ncdim_def(name = names(dims)[dim_i], units = "", vals = dim_vals)
    }
  }

  # get variable names
  var_names <- names(stars_obj)

  # get variable units
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

  # define variables for the NetCDF
  varis <-
    purrr::map2(
      var_names,
      var_units,
      ~ ncdf4::ncvar_def(name = .x, units = .y, dim = dims)
    )

  # create NetCDF file
  ncnew <-
    ncdf4::nc_create(filename = filename, vars = varis, force_v4 = TRUE)

  # add global attributes, if any
  if (!is.na(gatt_name)) {
    ncdf4::ncatt_put(ncnew, varid = 0, attname = gatt_name, attval = gatt_val)
  }

  # write data to the NetCDF file
  purrr::walk(
    seq_along(var_names),
    ~ ncdf4::ncvar_put(
      nc = ncnew,
      varid = varis[[.x]],
      vals = stars_obj |> dplyr::select(dplyr::all_of(.x)) |> dplyr::pull()
    )
  )

  # close the NetCDF file
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
  # check if a single point or a bounding box is provided
  if (is.na(xmax) & is.na(ymax)) {
    # if a single point, get the closest cell indices
    coords <-
      purrr::map2(c(xmin, ymin), c(1, 2), \(coord, dim_id) {
        # get dimension values
        s <-
          stars_obj |>
          stars::st_get_dimension_values(dim_id)

        # handle longitude conversion if necessary
        if (dim_id == 1 & max(s) > 180 & coord < 0) {
          which.min(abs(s - (360 + coord)))
        } else {
          which.min(abs(s - coord))
        }

        #
      }) |>
      purrr::set_names(c("x", "y"))

    # prepare the output list
    r <-
      list(x = coords$x, y = coords$y)
    #
  } else {
    # if a bounding box, get start and end indices
    coords <-
      purrr::map2(
        list(c(xmin, xmax), c(ymin, ymax)),
        c(1, 2),
        \(coords, dim_id) {
          # get dimension values
          s <-
            stars_obj |>
            stars::st_get_dimension_values(dim_id)

          # handle longitude conversion if necessary
          if (dim_id == 1 & max(s) > 180 & any(coords < 0)) {
            purrr::map(coords, \(x) which.min(abs(s - (360 + x))))
          } else {
            purrr::map(coords, \(x) which.min(abs(s - x)))
          }
          #
        }
      ) |>
      unlist(recursive = F) |>
      purrr::set_names(c("xmin", "xmax", "ymin", "ymax"))

    # ensure y_start is smaller than y_end
    if (coords$ymax > coords$ymin) {
      y_start = coords$ymin
      y_end = coords$ymax
    } else {
      y_start = coords$ymax
      y_end = coords$ymin
    }

    # prepare the output list with start, end, and count for each dimension
    r <-
      list(
        x_start = coords$xmin,
        y_start = y_start,
        x_end = coords$xmax,
        y_end = y_end,
        x_count = coords$xmax - coords$xmin + 1,
        y_count = coords$ymax - coords$ymin + 1
      )
  }

  return(r)
}
