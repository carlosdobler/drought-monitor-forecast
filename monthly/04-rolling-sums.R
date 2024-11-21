
# SCRIPT TO CALCULATE ROLLING SUMS OF MONTHLY WATER BALANCE

# OUTPUTS FILES WITH NAME: 
# era5_water-balance-rollsum3_mon_{date}.nc


date_i <- "2024-10-01"
date_f <- "2024-10-01"


# setup

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

if (exists(dir_data)){
  fs::dir_delete(dir_data)
}
fs::dir_create(dir_data)


dates_to_process <- 
  seq(as_date(date_i), as_date(date_f), by = "1 month")

# obtain all available input files
ff <- 
  str_glue("gsutil ls {dir_gs}/water_balance_th/") %>% 
  system(intern = T)

# obtain existing result files
ff_existing <- 
  str_glue("gsutil ls {dir_gs}/water_balance_th_rolled/") %>% 
  system(intern = T) %>% 
  fs::path_file()


walk(dates_to_process, \(d) {
  
  print(d)
  
  res_file <- str_glue("era5_water-balance-rollsum3_mon_{d}.nc")
  
  
  # the date to process has not been processed yet:
  if(!res_file %in% ff_existing){
    
    # tri-month period
    tri_mon <- seq(d-months(2), d, by = "1 month")
    
    
    # a loop has already passed
    # so there are only 2 files on disk
    if(length(fs::dir_ls(dir_data)) == 2) {
      
      # the 2 files are part of the tri-month period
      if(fs::dir_ls(dir_data) %>% str_sub(-13,-4) %>% {. %in% tri_mon} %>% all()) {
        
        # download only 1 file 
        # (last of tri-month)
        f <- 
          ff %>% 
          str_subset(as.character(last(tri_mon))) 
        
        str_glue("gsutil cp {f} {dir_data}") %>% 
          system(ignore.stdout = T, ignore.stderr = T)
        
      } else {
        
        # the 2 files are not part of the tri-month period
        # (date to process is not sequential):
        # delete both files
        fs::dir_ls(dir_data) %>% 
          fs::file_delete()
        
      }
      
    }
    
    
    # first loop
    # or the date to process is not sequential
    if(length(fs::dir_ls(dir_data)) == 0) {
      
      # download 3 files
      ff %>% 
        str_subset(str_flatten(tri_mon, "|")) %>% 
        future_walk(\(f){
          
          str_glue("gsutil cp {f} {dir_data}") %>% 
            system(ignore.stdout = T, ignore.stderr = T)
          
        })
      
    }
    
    
    # double-check files in disk are the right ones
    if(length(fs::dir_ls(dir_data)) == 3 &
       fs::dir_ls(dir_data) %>% str_sub(-13,-4) %>% {. %in% tri_mon} %>% all()) {
      
      # read files
      ff_disk <- 
        ff %>% 
        str_subset(str_flatten(tri_mon, "|")) %>% 
        fs::path_file() %>% 
        {str_glue("{dir_data}/{.}")}
      
      s <- 
        ff_disk %>% 
        future_map(read_ncdf) %>% 
        suppressMessages() %>% 
        {do.call(c, c(., along = "time"))}
      
      # sum ("rolling")
      r <- 
        s %>% 
        st_apply(c(1,2), sum, .fname = "wb", FUTURE = T) %>% 
        mutate(wb = wb %>% units::set_units(m))
      
      # write file to disk
      res_path <- 
        str_glue("{dir_data}/{res_file}")
      
      rt_write_nc(r,
                  res_path,
                  gatt_name = "source_code",
                  gatt_val = "https://github.com/carlosdobler/global-drought-monitor/tree/main/monthly")
      
      # upload to cloud
      str_glue("gsutil mv {res_path} {dir_gs}/water_balance_th_rolled/") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
      
      
      if(d == last(dates_to_process)) {
        
        fs::dir_delete(dir_data)
        
      } else {
        
        # remove first file
        ff_disk %>% 
          first() %>% 
          fs::file_delete()
        
      }
      
    } else {
      
      print("wrong files!")
      
    }
    
    
  } else {
    
    print("file exists - skipping!")
    
  }
  
})

