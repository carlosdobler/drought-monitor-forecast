#' Split a stars raster into tiles
#'
#' Function to split a stars object (can be a proxy) into tiles of a given size.
#' Output is a table specifying where each tile starts and ends in terms of cell positions.
#' If a mask is provided, an additional column is added indicating whether a tile overlaps
#' with cells with values from that mask. In this case, the output is an sf object (tiles
#' as polygons) with an associated table.
#'
#' @param s stars object or proxy
#' @param tile_size number of cells per side of tile
#' @param mask stars object in which areas of interest are represented with any value and
#' areas of no interest as NA. This object must have the exact same spatial dimensions as s.
#'
#' @export
rt_tile_table <- function(s, tile_size, mask = NULL) {
  # dimensions' indices
  dims <-
    c(1, 2) |>
    setNames(c("x", "y"))

  # create table of positions, one for each dimension
  df <-
    lapply(names(dims), function(dim_name) {
      dim_id <- dims[[dim_name]]

      d <-
        dim(s)[dim_id] |>
        seq_len()

      n <-
        round(dim(s)[dim_id] / tile_size)

      tiles <- split(d, ceiling(d / (length(d) / n)))

      do.call(
        rbind,
        lapply(tiles, function(.x) {
          data.frame(
            start = .x[1],
            end = .x[length(.x)],
            count = .x[length(.x)] - .x[1] + 1
          )
        })
      ) |>
        setNames(c(
          paste0("start_", dim_name),
          paste0("end_", dim_name),
          paste0("count_", dim_name)
        ))
    }) |>
    setNames(c("x", "y"))

  # combine both tables
  df <-
    expand.grid(
      seq_len(nrow(df$x)),
      seq_len(nrow(df$y))
    ) |>
    transform(
      start_x = df$x$start_x[Var1],
      end_x = df$x$end_x[Var1],
      count_x = df$x$count_x[Var1],
      start_y = df$y$start_y[Var2],
      end_y = df$y$end_y[Var2],
      count_y = df$y$count_y[Var2]
    ) |>
    subset(select = -c(Var1, Var2))

  # add index column with appropriate padding
  length_id <-
    df |>
    nrow() |>
    nchar()

  df <-
    df |>
    transform(
      tile_id = formatC(seq_len(nrow(df)), width = length_id, flag = "0")
    ) |>
    (\(x) x[c("tile_id", setdiff(names(x), "tile_id"))])()

  # if mask is provided:
  if (!is.null(mask)) {
    mask_data <-
      lapply(seq_len(nrow(df)), function(i) {
        start_x <- df$start_x[i]
        end_x <- df$end_x[i]
        start_y <- df$start_y[i]
        end_y <- df$end_y[i]

        # crop the mask raster to the tile
        mask_tile <-
          mask[,
            start_x:end_x,
            start_y:end_y
          ]

        # turn to polygon
        pol_tile <-
          mask_tile |>
          sf::st_bbox() |>
          sf::st_as_sfc() |>
          sf::st_sf(geom = _)

        # evaluate if there are areas of interest in the cropped mask tile
        pol_tile |>
          transform(mask = !all(is.na(mask_tile[[1]])))
      }) |>
      do.call(rbind, args = _)

    df <-
      cbind(df, mask_data) |>
      sf::st_as_sf()
  }

  return(df)
}


# *****

#' Load a tile from a list of rasters
#'
#' Loads a square area (a tile) of multiple rasters and concatenates them.
#' Loading happens in parallel by default using either mirai or future, depending on whether
#' daemons or a plan (e.g. multicore) have been called beforehand. If no daemons or
#' plan has been specified, downloads will be sequential. The option to force downloads
#' to be sequential is also available.
#'
#' @param df_matrix Matrix with one row per dimension and two columns:
#' one for start and another for count positions
#' @param list_files List of file paths to load
#' @param read_method "mdim" (default) to read data using stars::read_mdim or
#' "ncdf" to use stars::read_ncdf
#' @param parallel (boolean) TRUE downloads in parallel with mirai (if daemons have already been called) or
#' with future (if a plan has been specified). FALSE forces sequential downloads.
#'
#' @export
rt_tile_load <- function(
  df_matrix,
  list_files,
  read_method = "mdim",
  parallel = TRUE,
  quiet = TRUE,
  ...
) {
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

  # extract values from df_matrix for use in parallel workers
  start_vals <- df_matrix[, 1] |> as.integer()
  count_vals <- df_matrix[, 2] |> as.integer()

  f_import <- function(f_, ...) {
    if (read_method == "mdim") {
      s <-
        stars::read_mdim(
          f_,
          offset = start_vals - 1,
          count = count_vals
        )
    } else if (read_method == "ncdf") {
      s <-
        stars::read_ncdf(
          f_,
          ncsub = cbind(
            start = start_vals,
            count = count_vals
          )
        ) |>
        suppressMessages()
    }
    return(s)
  }

  # load files based on parallel engine
  if (parallel_ == "none") {
    # sequential loading
    if (!quiet) {
      message("   loading sequentially...")
    }

    s_tile <-
      list_files |>
      lapply(
        f_import,
        read_method = read_method,
        start_vals = start_vals,
        count_vals = count_vals
      )
  } else if (parallel_ == "m") {
    # parallel loading with mirai
    if (!quiet) {
      message("   loading in parallel (mirai)...")
    }

    s_tile <-
      list_files |>
      purrr::map(purrr::in_parallel(
        \(f) f_import(f),
        f_import = f_import,
        read_method = read_method,
        start_vals = start_vals,
        count_vals = count_vals
      ))
  } else if (parallel_ == "f") {
    # parallel loading with future
    if (!quiet) {
      message("   loading in parallel (future)...")
    }

    s_tile <-
      list_files |>
      furrr::future_map(
        f_import,
        read_method = read_method,
        start_vals = start_vals,
        count_vals = count_vals
      )
  }

  s_tile <-
    do.call(c, c(s_tile, ...))

  return(s_tile)
}


# *****

#' Mosaic a list of stars objects
#'
#' Combines multiple stars objects into a single mosaic, preserving units,
#' dimension names, and dimension values from the the original objects.
#'
#' @param list_s List of stars objects to mosaic
#' @param ... Additional arguments
#'
#' @export
rt_mosaic <- function(list_s, ...) {
  #
  vars <- names(list_s[[1]])

  r <-
    vars |>
    map(\(v) {
      list_s_1var <-
        list_s |>
        map(\(s) {
          s |>
            select({{ v }})
        })

      mos <-
        do.call(stars::st_mosaic, list_s_1var)

      if (inherits(list_s_1var[[1]][[1]], "units")) {
        un <- units::deparse_unit(list_s_1var[[1]][[1]])
        units(mos[[1]]) <- units::as_units(un)
      }

      n_dims <- length(dim(list_s[[1]]))

      mos <-
        stars::st_set_dimensions(
          mos,
          which = seq(n_dims),
          names = names(stars::st_dimensions(list_s[[1]]))
        )

      if (n_dims > 2) {
        for (n_dim in seq(n_dims) |> tail(-2)) {
          mos <-
            st_set_dimensions(
              mos,
              which = n_dim,
              values = st_get_dimension_values(list_s[[1]], which = n_dim)
            )
        }
      }

      return(mos)
    })

  if (length(vars) > 1) {
    r <- do.call(c, r)
  } else {
    r <- r[[1]]
  }

  return(r)
}
