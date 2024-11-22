
library(tidyverse)
library(stars)

{
  # crop <- c("maize", "coffee", "cocoa", "wheat", "cotton", "sugar")[1]
  # 
  # 
  # s_proxy <- 
  #   "/mnt/pers_disk/data_drought_monitor/era5-land_biweek_mean-daily-water-balance_1971-01-01.tif" %>% 
  #   read_stars()
  # 
  # if (crop == "maize") {
  #   
  #   crop_spam <- 
  #     "/mnt/bucket_cmip5/ag_risk_model/SPAM/2020/spam2020_v1r0_global_P_MAIZ_A.tif" %>% # only rainfed?
  #     read_stars()
  #   
  # } else if (crop == "cocoa") {
  #   
  #   crop_spam <-
  #     "/mnt/bucket_cmip5/ag_risk_model/SPAM/2020/spam2020_v1r0_global_P_COCO_A.tif" %>%
  #     read_stars()
  #   
  # } else if (crop == "coffee") {
  #   
  #   crop_spam <- 
  #     c("/mnt/bucket_cmip5/ag_risk_model/SPAM/2020/spam2020_v1r0_global_P_COFF_A.tif",
  #       "/mnt/bucket_cmip5/ag_risk_model/SPAM/2020/spam2020_v1r0_global_P_RCOF_A.tif") %>% 
  #     read_stars()
  #   
  #   crop_spam <- 
  #     crop_spam %>%
  #     merge() %>% 
  #     st_apply(c(1,2), sum, na.rm = T, .fname = "prod")
  #   
  #   # crop_spam <- 
  #   #   crop_spam %>% 
  #   #   mutate(prod = if_else(prod <= 0, NA, prod))
  #   
  # } else if (crop == "wheat") {
  #   
  #   crop_spam <-
  #     "/mnt/bucket_cmip5/ag_risk_model/SPAM/2020/spam2020_v1r0_global_P_WHEA_A.tif" %>%
  #     read_stars()
  #   
  # } else if (crop == "cotton") {
  #   
  #   crop_spam <- 
  #     "/mnt/bucket_cmip5/ag_risk_model/SPAM/2020/spam2020_v1r0_global_P_COTT_A.tif" %>%
  #     read_stars()
  #   
  # } else if (crop == "sugar") {
  #   
  #   crop_spam <- 
  #     c("/mnt/bucket_cmip5/ag_risk_model/SPAM/2020/spam2020_v1r0_global_P_SUGB_A.tif",
  #       "/mnt/bucket_cmip5/ag_risk_model/SPAM/2020/spam2020_v1r0_global_P_SUGC_A.tif") %>% 
  #     read_stars()
  #   
  #   crop_spam <- 
  #     crop_spam %>%
  #     merge() %>% 
  #     st_apply(c(1,2), sum, na.rm = T, .fname = "prod")
  #   
  #   # crop_spam <-
  #   #   crop_spam %>%
  #   #   mutate(prod = if_else(prod <= 0, NA, prod))
  #   
  # }
  # 
  # 
  # 
  # crop_spam <- 
  #   crop_spam %>% 
  #   setNames("r") %>% 
  #   mutate(r = if_else(is.na(r), 0, r)) %>% 
  #   st_warp(s_proxy, use_gdal = T, method = "med") %>% 
  #   setNames("spam")
  # 
  # crop_spam %>% 
  #   pull() %>% 
  #   .[. > 0] %>% 
  #   quantile(0.1, na.rm = T) -> th
  # 
  # 
  # 
  # crop_spam_f <- 
  #   crop_spam %>% 
  #   mutate(spam = if_else(spam < th, NA, 1L))
  # 
  # 
  # pol <- 
  #   crop_spam_f %>%
  #   st_as_sf(as_points = F, merge = T)
  # 
  # write_sf(pol, str_glue("/mnt/bucket_mine/misc_data/spam/{crop}.gpkg"), append = F)
  # 
  # 
  # 
  # # ***********
  # 
  # crops <- c("maize", "coffee", "cocoa", "wheat", "cotton", "sugar")
  # 
  # crops %>% 
  #   walk(function(crop){
  #     
  #     str_glue("gsutil cp gs://clim_data_reg_useast1/misc_data/spam/{crop}.gpkg gs://drought-monitor/input_data/vector/") %>% 
  #       system()
  #     
  #   })
}


crops <- 
  c("BARL", "SOYB") %>% 
  set_names(c("barley", "soybean"))

# download file for proxy
str_glue("gsutil cp gs://clim_data_reg_useast1/era5/monthly_means/water_balance_th/era5_water-balance_mon_1970-01-01.nc .") %>% 
  system()

s_proxy <-
  "era5_water-balance_mon_1970-01-01.nc" %>%
  read_ncdf()

fs::file_delete("era5_water-balance_mon_1970-01-01.nc")



iwalk(crops, \(crop, i){
  
  print(i)
  
  f_crop <- str_glue("spam2020_v1r0_global_P_{crop}_A.tif")
  
  str_glue("gsutil cp gs://clim_data_reg_useast1/misc_data/agriculture/spam/spam2020/production/{f_crop} .") %>% 
    str_glue() %>% 
    system()
  
  s_crop <-
    read_stars(f_crop) %>% 
    setNames("r") %>% 
    mutate(r = if_else(is.na(r), 0, r)) %>%
    st_warp(s_proxy, use_gdal = T, method = "med") %>%
    setNames("spam")
  
  fs::file_delete(f_crop)
  
  th <- 
    s_crop %>%
    pull() %>%
    .[. > 0] %>%
    quantile(0.1, na.rm = T)
  
  s_crop_f <- 
    s_crop %>%
    mutate(spam = if_else(spam < th, NA, 1L))
  
  pol <-
    s_crop_f %>%
    st_as_sf(as_points = F, merge = T)

  write_sf(pol, str_glue("{i}.gpkg"), append = F)
  
  str_glue("gsutil mv {i}.gpkg gs://drought-monitor/input_data/vector/") %>% 
    system()
  
})











