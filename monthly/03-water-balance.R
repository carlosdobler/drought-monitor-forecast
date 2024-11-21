
# SCRIPT TO CALCULATE MONTHLY WATER BALANCE

# OUTPUTS FILES WITH NAME: 
# era5_water-balance_mon_{date}.nc

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
dir_data <- "/mnt/pers_disk/tmp/"

if (exists(dir_data)){
  fs::dir_delete(dir_data)
}
fs::dir_create(dir_data)


dates_to_process <- 
  seq(as_date(date_i),
      as_date(date_f),  
      by = "1 month")


# obtain input file names 
ff <-
  c("total_precipitation", "potential_evapotranspiration_th") %>% 
  map(\(v){
    
    str_glue("gsutil ls {dir_gs}/{v}") %>% 
      system(intern = T) %>% 
      str_subset(str_flatten(dates_to_process, "|")) 
    
  })

# obtain existing result file names
ff_existing <-
  str_glue("gsutil ls {dir_gs}/water_balance_th/") %>%
  system(intern = T) %>%
  fs::path_file()




# loop through dates
walk(dates_to_process, \(d){
  
  print(d)
  
  res_file <- str_glue("era5_water-balance_mon_{d}.nc")
  
  # the date to process has not been processed yet:
  if(!res_file %in% ff_existing){
    
    f_pr <- ff[[1]] %>% str_subset(as.character(d))
    f_pet <- ff[[2]] %>% str_subset(as.character(d))
    
    # copy files to disk
    future_walk(c(f_pr, f_pet), \(f){
      
      str_glue("gsutil cp {f} {dir_data}") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
      
    })
    
    # import and calculate water balance
    s_wb <-
      c(f_pr, f_pet) %>% 
      fs::path_file() %>% 
      {str_glue("{dir_data}/{.}")} %>% 
      future_map(read_ncdf) %>% 
      suppressMessages() %>% 
      do.call(c, .) %>% 
      mutate(pet = pet %>% units::set_units(m),
             wb = tp - pet) %>% 
      select(wb)
    
    # save result as ncdf in disk
    res_path <- 
      str_glue("{dir_data}/{res_file}")
    
    rt_write_nc(s_wb,
                res_path,
                gatt_name = "source_code",
                gatt_val = "https://github.com/carlosdobler/global-drought-monitor/tree/main/monthly")
    
    # upload to gcloud
    str_glue("gsutil mv {res_path} {dir_gs}/water_balance_th/") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    # clean up
    fs::dir_ls(dir_data) %>% 
      fs::file_delete()
    
  } else {
    
    print("file exists - skipping!")
    
  }
  
  if(d == last(dates_to_process)) {
    fs::dir_delete(dir_data)
  }
  
})









