
# SCRIPT TO CALCULATE MONTHLY WATER BALANCE



# SETUP

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)

plan(multicore)

source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

dir_gs <- "gs://clim_data_reg_useast1/era5/monthly_means"
dir_data <- "/mnt/pers_disk/tmp"
fs::dir_create(dir_data)

yrs <- c(1981,2000)


# extract file names 
ff <-
  c("total_precipitation", "potential_evaporation") %>% 
  map(\(v){
    
    str_glue("gsutil ls {dir_gs}/{v}") %>% 
      system(intern = T) %>% 
      str_subset(str_flatten((yrs[1]-1):yrs[2], "|"))
    
  })
  
  

seq(length(ff[[1]])) %>% 
  walk(\(f_in){
    
    # make sure both files are from the same date
    if(str_sub(ff[[1]][f_in], -13) == str_sub(ff[[2]][f_in], -13)) {
      
      str_glue("gsutil cp {ff[[1]][f_in]} {dir_data}") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
      
      str_glue("gsutil cp {ff[[2]][f_in]} {dir_data}") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
        
      "cdo 
      -setattribute,Source_code=github 
      -sub {dir_data}/{fs::path_file(ff[[1]][f_in])} 
      {dir_data}/{fs::path_file(ff[[2]][f_in])} 
      {dir_data}/era5_water-balance_mon_{str_sub(ff[[1]][f_in], -13, -4)}.nc" %>%
        str_squish() %>% 
        str_glue() %>% 
        system(ignore.stderr = T, ignore.stdout = T)
      
    }
    
  })




