# 🌍 Global Drought Monitor and Forecast

This project generates monthly water balance conditions and forecasts to support drought monitoring and prediction. The pipeline consists of two main components:

1.  **Current Conditions (Monitor)**: Processes ERA5 reanalysis data to generate monthly water balance anomalies
2.  **Future Conditions (Forecast)**: Uses NMME climate forecasts to predict water balance anomalies 6 months into the future

## ⭐ Key Features

-   Calculates water balance anomalies using a modified SPEI methodology
-   Bias-corrects NMME forecasts using ERA5 climatology
-   Generates probabilistic forecasts from multiple NMME ensemble members
-   Provides results at 3- and 12-month time scales (integration windows)
-   Outputs standardized anomalies as percentiles relative to a 1991-2020 baseline

## 🔧 Pipeline Components

### 📊 Monitor Generator

-   Downloads ERA5 precipitation and temperature data
-   Calculates monthly water balance
-   Aggregates temporally to obtain different time scales
-   Computes standardized anomalies using log-logistic distribution
-   Outputs percentile maps showing current conditions

### 🔮 Forecast Generator

-   Downloads and bias-corrects NMME temperature and precipitation data
-   Calculates water balance for each ensemble member
-   Integrates with historical ERA5 data to obtain different time scales
-   Generates probabilistic 6-month forecasts (standardized anomalies)
-   Outputs forecast percentile maps with uncertainty metrics

## 🚀 Usage

The pipeline is designed to run monthly: `source("monitor_forecast/RUN_PIPELINE.R")`
