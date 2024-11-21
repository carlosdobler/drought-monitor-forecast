
# SCRIPT TO CALCULATE MONTHLY POTENTIAL EVAPOTRANSPIRATION (THORNWHAITE)


# Reference for daylight coefficients
# https://github.com/sbegueria/SPEI/blob/master/R/thornthwaite.R#L128-L140


date_i <- "2024-10-01"
date_f <- "2024-10-01"


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

# delete if dir exists (clean slate)
if (exists(dir_data)){
  fs::dir_delete(dir_data)
}

fs::dir_create(dir_data)

dates_to_process <- 
  seq(as_date(date_i),
      as_date(date_f),  
      by = "1 month")


# download and import annual heat index
"gsutil cp gs://clim_data_reg_useast1/era5/climatologies/era5_heat-index_yr_1991-2020.nc {dir_data}" %>% 
  str_glue() %>% 
  system(ignore.stderr = T, ignore.stdout = T)

s_hi <- 
  str_glue("{dir_data}/era5_heat-index_yr_1991-2020.nc") %>% 
  read_ncdf() %>% 
  mutate(ann_h_ind = round(ann_h_ind, 2))


# calculate alpha
s_alpha <- 
  s_hi %>% 
  mutate(alpha = (6.75e-7 * ann_h_ind^3) - (7.71e-5 * ann_h_ind^2) + 0.01792 * ann_h_ind + 0.49239) %>% 
  select(alpha)


# calculate daylight duration
K_mon <- 
  map2(seq(15,365, by = 30), # Julian day (mid-point)
       c(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31), # days/month
       \(J, days_in_mon){
         
         s_hi %>% 
           st_dim_to_attr(2) %>% 
           mutate(tanLat = tan(latitude/57.2957795),
                  Delta = 0.4093 * sin(((2 * pi * J) / 365) - 1.405),
                  tanDelta = tan(Delta),
                  tanLatDelta = tanLat * tanDelta,
                  tanLatDelta = ifelse(tanLatDelta < (-1), -1, tanLatDelta),
                  tanLatDelta = ifelse(tanLatDelta > 1, 1, tanLatDelta),
                  omega = acos(-tanLatDelta),
                  N = 24 / pi * omega,
                  K = N / 12 * days_in_mon / 30) %>% 
           select(K)
         
  })


# obtain input file names 
ff <-
  str_glue("gsutil ls {dir_gs}/2m_temperature") %>% 
      system(intern = T) %>% 
      str_subset(str_flatten(dates_to_process, "|")) 
  
# obtain existing output file names
ff_existing <- 
  str_glue("gsutil ls {dir_gs}/potential_evapotranspiration_th/") %>% 
  system(intern = T) %>% 
  fs::path_file()



# loop through dates
walk(dates_to_process, \(d){
  
  print(d)
  
  mon <- month(d)
  
  res_file <- str_glue("era5_potential-evapotranspiration-th_mon_{d}.nc")
  
  # the date to process has not been processed yet:
  if(!res_file %in% ff_existing){
    
    # copy file to disk
    f <- ff %>% str_subset(as.character(d))
    
    str_glue("gsutil cp {f} {dir_data}") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    f <- 
      f %>% 
      fs::path_file() %>% 
      {str_glue("{dir_data}/{.}")}
    
    # import 
    s_tas <- 
     f %>% 
      read_ncdf() %>% 
      suppressMessages() %>% 
      adrop() %>% 
      mutate(t2m = t2m %>% units::set_units(degC)) %>% 
      units::drop_units()
    
    # calculate PET
    s_pet <- 
      c(s_tas, s_hi, s_alpha, pluck(K_mon, mon)) %>% 
      
      mutate(t2m = if_else(t2m < 0, 0, t2m),
             pet = K * 16 * (10 * t2m / ann_h_ind)^alpha,
             pet = if_else(is.na(pet) | is.infinite(pet), 0, pet),
             pet = pet %>% units::set_units(mm),
             pet = pet/(c(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)[mon])
             ) %>% 
      select(pet)
    
    
    # save output
    rt_write_nc(s_pet,
                str_glue("{dir_data}/{res_file}"),
                gatt_name = "source_code",
                gatt_val = "https://github.com/carlosdobler/global-drought-monitor/tree/main/monthly")
    
    "gsutil mv {dir_data}/{res_file} gs://clim_data_reg_useast1/era5/monthly_means/potential_evapotranspiration_th/" %>% 
      str_glue() %>% 
      system()

    fs::file_delete(f)
    
    
  } else {
    
    print("file exists -- skipping!")
    
  }
  
  # clean up
  if(d == last(dates_to_process)) {
    fs::dir_delete(dir_data)
  }
  
})
      
      
      
      
      
      
      
      
    
    
      
    

    

