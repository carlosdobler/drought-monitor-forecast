
# FIX BUCKET ROUTES!!!
# CHANGE TO CHECK FOR EXISTING FILES



# SCRIPT TO CALCULATE WATER BALANCE PERCENTILES
# OUTPUTS FILES WITH NAME: 
# era5_water-balance-perc-w3_bl-XXXX-XXXX_mon_{date}.nc

date_i <- "2024-10-01"
date_f <- "2024-10-01"

bl_yrs <- c(1991, 2020)



library(tidyverse)
library(stars)
library(furrr)
options(future.fork.enable = T)
plan(multicore)


# load function to save ncdfs
source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

# key directories
dir_gs <- "gs://clim_data_reg_useast1/era5/monthly_means"
dir_data <- "/mnt/pers_disk/tmp/"
if (exists(dir_data)){
  fs::dir_delete(dir_data)
}
fs::dir_create(dir_data)


dates_to_process <- 
  seq(as_date(date_i), as_date(date_f), by = "1 month")

# obtain input files
ff <- 
  str_glue("gsutil ls {dir_gs}/water_balance_th_rolled/") %>% 
  system(intern = T) %>% 
  str_subset("rollsum3")

ff_sub <- 
  ff %>% 
  str_subset(
    c(str_glue("_{bl_yrs[1]:bl_yrs[2]}-"),
      as.character(dates_to_process)) %>% 
      str_flatten("|")
    )

# obtain existing output files
ff_existing <- 
  str_glue("gsutil ls {dir_gs}/water_balance_th_perc/") %>% 
  system(intern = T) %>% 
  str_subset(str_glue("{bl_yrs[1]}-{bl_yrs[2]}")) %>% 
  fs::path_file()


# land mask
str_glue("gsutil cp {ff_sub[1]} {dir_data}") %>% 
  system(ignore.stdout = T, ignore.stderr = T)

f_proxy <- 
  dir_data %>% 
  fs::dir_ls()

s_proxy <- 
  f_proxy %>% 
  read_ncdf() %>% 
  suppressMessages()

fs::file_delete(f_proxy)


str_glue("gsutil cp gs://clim_data_reg_useast1/misc_data/physical/ne_110m_land/* {dir_data}") %>% 
  system(ignore.stdout = T, ignore.stderr = T)

land_p <- 
  "{dir_data}ne_110m_land.shp" %>%
  str_glue() %>% 
  read_sf()

dir_data %>% 
  fs::dir_ls(regexp = "ne_110") %>% 
  fs::file_delete()

land_r <- 
  land_p %>% 
  mutate(a = 1) %>% 
  select(a) %>% 
  st_rasterize(
    st_as_stars(st_bbox(),
                xlim = c(st_bbox(s_proxy)[1] - 180, st_bbox(s_proxy)[3] - 180),
                ylim = c(st_bbox(s_proxy)[2], st_bbox(s_proxy)[4]),
                dx = 0.11,
                values = 0)
  ) %>%
  st_warp(st_as_stars(st_bbox(),
                      xlim = c(st_bbox(s_proxy)[1], st_bbox(s_proxy)[3]),
                      ylim = c(st_bbox(s_proxy)[2], st_bbox(s_proxy)[4]),
                      dx = 0.11,
                      values = 0))

land_r <- 
  land_r %>% 
  st_warp(s_proxy,
          method = "max",
          use_gdal = T) %>% 
  setNames("land") %>% 
  suppressWarnings()

land_r[land_r == 0] <- NA

st_dimensions(land_r) <- st_dimensions(s_proxy)[1:2]


# ********

fs::dir_ls(dir_data)

mons <- dates_to_process %>% str_sub(-5,-4) %>% unique()

walk(mons, \(mon){
  
  print(mon)
  
  # files for month
  ff_mon <- 
    ff_sub %>% 
    str_subset(str_glue("-{mon}-")) %>% 
    fs::path_file()
  
  exist <- 
    ff_mon %>% 
    str_subset(str_flatten(dates_to_process, "|")) %>%
    str_sub(-13) %>% 
    {. %in% str_sub(ff_existing, -13)}
  
  
  if(!all(exist)){
    
    # download all relevant files (bl + input)
    # (fix this to download only necessary files)
    ff_mon %>% 
      future_walk(\(f){
        
        str_glue("gsutil cp {dir_gs}/water_balance_th_rolled/{f} {dir_data}") %>% 
          system(ignore.stdout = T, ignore.stderr = T)
        
      })
    
    # import baseline
    ss_bl <- 
      str_glue("{dir_data}/{ff_mon}") %>% 
      str_subset(str_flatten(bl_yrs[1]:bl_yrs[2], "|")) %>% 
      future_map(read_ncdf) %>% 
      suppressMessages()
    
    
    dd <- 
      dates_to_process[month(dates_to_process) == as.numeric(mon)]
    
    
    walk2(dd, exist, \(d, ex){
      
      print(d)
      
      if(!ex) {
        
        # read month
        s <- 
          ff_mon %>% 
          str_subset(as.character(d)) %>%
          {str_glue("{dir_data}/{.}")} %>% 
          read_ncdf() %>% 
          suppressMessages()
        
        r <- 
          c(list(s), ss_bl) %>% # concatenate month + baseline
          {do.call(c, c(., along = "time"))} %>% 
          st_apply(c(1,2), \(x){
            
            round(ecdf(x[-1])(x[1]), 3) # percentiles
            
          },
          .fname = "perc",
          FUTURE = T)
        
        r[is.na(land_r)] <- NA # remove oceans
        
        # save result as ncdf in disk
        res_file <- 
          str_glue("era5_water-balance-perc-w3_bl-{bl_yrs[1]}-{bl_yrs[2]}_mon_{d}.nc")
        
        res_path <- 
          str_glue("{dir_data}/{res_file}")
        
        rt_write_nc(r,
                    res_path,
                    gatt_name = "source_code",
                    gatt_val = "https://github.com/carlosdobler/global-drought-monitor/tree/main/monthly")
        
        # upload to gcloud
        # str_glue("gsutil mv {res_path} {dir_gs}/water_balance_th_perc/") %>% 
        #   system(ignore.stdout = T, ignore.stderr = T)
        str_glue("gsutil mv {res_path} gs://drought-monitor/input_data/raster_monthly/") %>%
          system(ignore.stdout = T, ignore.stderr = T)
        
        
      } else {
        
        print("existing date - skipping!")
        
      } 
      
    })
    
    dir_data %>% 
      fs::dir_ls() %>% 
      fs::file_delete()
    
  } else {
    
    print("all dates for this month exist - skipping!")
    
  }
  
  
  if(mon == last(mons)){
    
    fs::dir_delete(dir_data)
    
  }
  
  
})



# # VERIFY 10 dates
# 
# fs::dir_create(dir_data)
# 
# d <-
#   sample(dates_to_process, 10)
# 
# ff <-
#   str_glue("gsutil ls {dir_gs}/water_balance_tH_perc/") %>%
#   system(intern = T) %>%
#   str_subset(str_flatten(d, "|"))
# 
# ff %>%
#   future_walk(\(f){
# 
#     str_glue("gsutil cp {f} {dir_data}") %>%
#       system(ignore.stdout = T, ignore.stderr = T)
# 
#   })
# 
# ff2 <-
#   ff %>%
#   fs::path_file() %>%
#   {str_glue("{dir_data}/{.}")}
# 
# i = 8
# 
# {
#   print(ff2[i])
# 
#   s <-
#   read_ncdf(ff2[i])
# 
# s[is.na(land_r)] <- NA
# 
# s %>% as_tibble() %>%
#   ggplot(aes(longitude, latitude, fill = perc)) +
#   geom_raster() +
#   colorspace::scale_fill_binned_diverging(na.value = "transparent",
#                                            rev = T,
#                                           mid = 0.5,
#                                           n.breaks = 9)
# }
# 
# fs::dir_delete(dir_data)
