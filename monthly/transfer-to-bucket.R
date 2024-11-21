
# NOT NEEDED ANYMORE !!!


library(tidyverse)
library(furrr)
options(future.fork.enable = T)
plan(multicore)

# key directories
dir_gs <- "gs://clim_data_reg_useast1/era5/monthly_means"
dir_data <- "/mnt/pers_disk/tmp"
fs::dir_delete(dir_data)
fs::dir_create(dir_data)

ff <- 
  str_glue("gsutil ls {dir_gs}/water_balance_tH_perc") %>% 
  system(intern = T)

ff <- 
  ff %>% 
  str_subset("1991-2020") %>% 
  str_subset(str_glue("_{2000:2024}-\\d{{2}}") %>% str_flatten("|"))

ff %>% 
  future_walk(\(f){
    str_glue("gsutil cp {f} {dir_data}") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
  })


ff <- 
  ff %>% 
  fs::path_file() %>% 
  {str_glue("{dir_data}/{.}")}


ff %>% 
  walk(\(f){
    
    "gsutil mv {f} gs://drought-monitor/input_data/raster_monthly/" %>% 
      str_glue() %>% 
      system()
    
  })


fs::dir_delete(dir_data)