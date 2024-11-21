
reticulate::use_condaenv("risk")
# needs cdsapi python package and file ~/.cdsapirc

date_i <- "2024-10-01"
date_f <- "2024-10-01"


source("https://raw.github.com/carlosdobler/spatial-routines/master/download_data.R")

library(tidyverse)

dir_gs <- "gs://clim_data_reg_useast1/era5/monthly_means"

dates_to_download <- 
  seq(as_date(date_i), as_date(date_f), by = "1 month")


# obtain names of existing files
ff <-
  c("total_precipitation", "2m_temperature") %>% 
  map(\(v){
    
    str_glue("gsutil ls {dir_gs}/{v}") %>% 
      system(intern = T) 
    
  })


# precipitation
exist_pr <- 
  ff[[1]] %>% 
  str_sub(-13, -4) %>% 
  {dates_to_download %in% .} 
  
if(!all(exist_pr)) {
  
  dd <- dates_to_download[!exist_pr]
  
  rt_download_era_monthly("total_precipitation",
                          year(dd),
                          month(dd),
                          dest_dir = str_glue("{dir_gs}/total_precipitation/")
  )
}


# temperature
exist_t2m <- 
  ff[[2]] %>% 
  str_sub(-13, -4) %>% 
  {dates_to_download %in% .} 

if(!all(exist_t2m)) {
  
  dd <- dates_to_download[!exist_t2m]
  
  rt_download_era_monthly("2m_temperature",
                          year(dd),
                          month(dd),
                          dest_dir = str_glue("{dir_gs}/2m_temperature/")
  )
}







