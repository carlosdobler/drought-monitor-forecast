
# table of data sources
df_sources <- 
  
  bind_rows(
    
    tibble(model = "canesm5",          end_hindcast = as_date("2020-12-01"), model_cast_url = "CanSIPS-IC4/.CanESM5/.{cast}"),
    tibble(model = "gem5p2-nemo",      end_hindcast = as_date("2020-12-01"), model_cast_url = "CanSIPS-IC4/.GEM5.2-NEMO/.{cast}"),
    tibble(model = "gfdl-spear",       end_hindcast = as_date("2020-12-01"), model_cast_url = "GFDL-SPEAR/.{cast}"),
    tibble(model = "cola-rsmas-cesm1", end_hindcast = NA,                    model_cast_url = "COLA-RSMAS-CESM1"),
    tibble(model = "cola-rsmas-ccsm4", end_hindcast = NA,                    model_cast_url = "COLA-RSMAS-CCSM4"),
    tibble(model = "nasa-geoss2s",     end_hindcast = as_date("2017-01-01"), model_cast_url = "NASA-GEOSS2S/.{cast}"),
    tibble(model = "ncep-cfsv2",       end_hindcast = as_date("2011-03-01"), model_cast_url = "NCEP-CFSv2/.{cast}")
    
  )