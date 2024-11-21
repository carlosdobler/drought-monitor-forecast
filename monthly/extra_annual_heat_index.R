

library(tidyverse)
library(stars)
library(furrr)
options(future.fork.enable = T)
plan(multicore)


# load function to save ncdfs
source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

# key directories
dir_gs <- "gs://clim_data_reg_useast1/era5/monthly_means"
dir_data <- "/mnt/pers_disk/tmp"
fs::dir_delete(dir_data)
fs::dir_create(dir_data)



ff <- 
  "gsutil ls {dir_gs}/2m_temperature/" %>% 
  str_glue() %>% 
  system(intern = T) %>% 
  str_subset(str_flatten(1991:2020, "|"))


# download
future_walk(ff, \(f) {
  
  "gsutil cp {f} {dir_data}" %>% 
    str_glue() %>% 
    system(ignore.stdout = T, ignore.stderr = T)
  
})

# update list of files
ff <- 
  ff %>% 
  fs::path_file() %>% 
  {str_glue("{dir_data}/{.}")}



s_monthly_h_ind <- 
  seq(12) %>% 
  str_pad(2, "left", "0") %>% 
  map(\(mon){
    
    s <- 
      ff %>% 
      str_subset(str_glue("-{mon}-")) %>% 
      future_map(read_ncdf, proxy = F) %>% 
      suppressMessages() %>% 
      map(adrop)
    
    s <- 
      do.call(c, c(s, along = "time"))
    
    s_mon <- 
      s %>%
      mutate(t2m = t2m %>% units::set_units(degC)) %>% 
      st_apply(c(1,2), mean, .fname = "t2m", FUTURE = T)
    
    s_h_ind <- 
      s_mon %>% 
      mutate(i = if_else(t2m < 0, 0, (t2m/5)^1.514)) %>% 
      select(i)
    
    return(s_h_ind)
    
  })


s_ann_h_ind <- 
  do.call(c, c(s_monthly_h_ind, along = "mon")) %>% 
  st_apply(c(1,2), sum, .fname = "ann_h_ind", FUTURE = T)


res_file <- "era5_heat-index_yr_1991-2020.nc"

rt_write_nc(s_ann_h_ind,
            str_glue("{dir_data}/{res_file}"),
            gatt_name = "source_code",
            gatt_val = "https://github.com/carlosdobler/global-drought-monitor/tree/main/monthly")

"gsutil mv {dir_data}/{res_file} gs://clim_data_reg_useast1/era5/climatologies/" %>% 
  str_glue() %>% 
  system()

fs::dir_delete(dir_data)


# # test
# 
# s <-
#   ff %>%
#   str_subset(str_flatten(c(1990,2000), "|")) %>%
#   map(read_ncdf) %>%
#   suppressMessages() %>%
#   map(adrop) %>%
#   {do.call(c, c(., along = "time"))}
# 
# s <-
#   s %>%
#   mutate(t2m = t2m %>% units::set_units(degC)) %>%
#   units::drop_units()
# 
# lat <- 60
# lat_in <- which.min(abs(st_get_dimension_values(s,2) - lat))
# 
# xx <- s %>% pull() %>% .[50,lat_in,]
# 
# SPEI::thornthwaite(ts(xx, frequency = 12), lat = lat) %>% round(2)
# 
# 
# x <- xx
# x[x < 0] <- 0
# m <- matrix(x, ncol = 12, byrow = T)
# 
# heat_ind <-
#   m %>%
#   apply(2, mean) %>%
#   {(./5)^1.514}
# 
# heat_ind <-
#   heat_ind %>%
#   sum()
# 
# 
# "gsutil cp gs://clim_data_reg_useast1/era5/climatologies/era5_heat-index_yr_1971-2000.nc {dir_data}" %>% 
#   str_glue() %>% 
#   system()
# 
# s_ann_h_ind <- 
#   str_glue("{dir_data}/era5_heat-index_yr_1971-2000.nc") %>% 
#   read_ncdf()
# 
# heat_ind <- 
#   s_ann_h_ind %>% pull() %>% .[50,lat_in]
# 
# alpha <-
#   (6.75e-7 * heat_ind^3) - (7.71e-5 * heat_ind^2) + 0.01792 * heat_ind + 0.49239
# 
# days_month <- seq(15,365, by = 30)
# days_in_mon <- c(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
# 
# tanLat <- tan(lat / 57.2957795)
# # mean solar declination angle for each month (Delta)
# Delta <- 0.4093 * sin(((2 * pi * days_month) / 365) - 1.405)
# # hourly angle of sun rising (omega)
# tanDelta <- tan(Delta)
# tanLatDelta <- tanLat * tanDelta
# tanLatDelta <- ifelse(tanLatDelta < (-1), -1, tanLatDelta)
# tanLatDelta <- ifelse(tanLatDelta > 1, 1, tanLatDelta)
# omega <- acos(-tanLatDelta)
# # mean daily daylight hours for each month (N)
# N <- 24 / pi * omega
# # which leads to K
# K <- N / 12 * days_in_mon / 30
# 
# PET <- K * 16 * (10 * x / heat_ind)^alpha
# PET %>% matrix(ncol = 12, byrow = T) %>% round(2)


