# Libraries -----------------------------------------------------------------
library(tidyverse)
library(openxlsx)
library(tidytransit)
library(psrcelmer)
library(psrccensus)
library(tidycensus)
library(sf)
library(rmapshaper)

source("functions.R")

# Inputs ------------------------------------------------------------------
wgs84 <- 4326
spn <- 2285

gtfs_years <- c(seq(2016, 2024, by=1))
gtfs_service <- "fall"
latest_census <- 2023
latest_ofm <- 2024
first_yr <- 2010

generate_ntd_data <- "yes"
generate_gtfs_data <- "no"
generate_efa_data <- "no"
generate_parcel_data <- "no"
generate_transit_population <- "no"
generate_transit_buffers <- "no"
generate_transit_layers <- "no"
generate_transit_trip_population <- "no"
generate_transit_trip_buffers <- "no"
generate_heat_map_buffers <- "no"

hct_modes <- c("BRT", "Passenger Ferry", "Light Rail", "Streetcar", "Commuter Rail", "Auto Ferry")
bus_modes <- c("Bus", "ST Express")
ferry_modes <- c("Passenger Ferry", "Auto Ferry")
rail_modes<- c("Light Rail", "Streetcar")
transit_modes <- c("Bus", "ST Express", "BRT", "Passenger Ferry", "Light Rail", "Streetcar", "Commuter Rail", "Auto Ferry")
geography_order = c("City of Seattle", "Community Transit", "Everett Transit", "King County Metro", "Kitsap Transit", "Pierce County Ferries", "Pierce Transit", "Sound Transit", "Washington State Ferries", "Region")

# NTD Data ----------------------------------------------------
if (generate_ntd_data == "yes") {
  
  # Monthly Ridership Data
  ntd_ridership_data <- process_ntd_ridership_data()
  
  # Annual Expense Data
  ntd_operating_expenses <- process_ntd_operating_expense_data(start_yr = first_yr)
  
  # Annual Capital Expenditures
  ntd_capital_expenditures <- process_ntd_capital_expenditures(start_yr = first_yr)
  
  # Annual Operations & Capital Funds by Source
  ntd_operations_capital_funds <- process_ntd_operating_and_capital_funding(start_yr = first_yr) 
  
  # Combine NTD data and output
  ntd_data <- bind_rows(ntd_ridership_data, ntd_operating_expenses, ntd_capital_expenditures, ntd_operations_capital_funds)
  saveRDS(ntd_data, "data/ntd_data.rds")
  rm(ntd_ridership_data, ntd_operating_expenses, ntd_capital_expenditures, ntd_operations_capital_funds)
  
} else {
  print(str_glue("Loading NTD Summary Data from stored results."))
  ntd_data <- readRDS("data/ntd_data.rds")
}

# GTFS Data ---------------------------------------------------------------
if (generate_gtfs_data == "yes") {
  transit_stops <- NULL
  for(y in gtfs_years) {
    s <- transit_stops_by_mode(year = y, service_change = gtfs_service)
    if(is.null(transit_stops)) {transit_stops <- s} else {transit_stops <- bind_rows(transit_stops, s)}
    rm(s)
  }
  transit_stops <- transit_stops |> drop_na()
  saveRDS(transit_stops, "data/transit_stops.rds")
} else {
  print(str_glue("Loading GTFS based transit stop Data from stored results."))
  transit_stops <- readRDS("data/transit_stops.rds")
}

# EFA shares by Tract ----------------------------------
if (generate_efa_data == "yes") {
  
  tract_efa_data <- NULL
  for(y in gtfs_years) {
    
    poc <- acs_equity_shares(year=y, census_metric = "poc")
    poverty <- acs_equity_shares(year=y, census_metric = "poverty")
    youth <- acs_equity_shares(year=y, census_metric = "youth")
    older <- acs_equity_shares(year=y, census_metric = "older")
    lep <- acs_equity_shares(year=y, census_metric = "lep")
    disability <- acs_equity_shares(year=y, census_metric = "disability")
    
    p <- list(poc, poverty, youth, older, lep, disability) |> 
      reduce(left_join, by = c("tract", "census_year", "data_year")) |>
      select(tract_geoid = "tract", "census_year", "data_year", "poc", "poverty", "youth", "older", "lep", "disability")
    
    if (is.null(tract_efa_data)) {tract_efa_data <- p} else {tract_efa_data <- bind_rows(tract_efa_data, p)}
    rm(p, poc, poverty, youth, older, lep, disability)
  }
  
  saveRDS(tract_efa_data, "data/tract_efa_data.rds")
  
} else {
  print(str_glue("Loading EFA Tract Data from stored results."))
  tract_efa_data <- readRDS("data/tract_efa_data.rds")
  
}

# Parcel Population -------------------------------------------------------
if(generate_parcel_data == "yes") {
  
  parcel_data <- NULL
  for(y in gtfs_years) {
    
    # If latest GTFS data is newer than latest OFM data, use the most recent OFM data
    if (y > latest_ofm) {yr <- latest_ofm} else {yr <- y}
  
    # Parcel population
    print(str_glue("Loading {yr} OFM based parcelized estimates of total population"))
    if (yr < 2020) {ofm_vintage <- 2020} else {
      if(yr < 2023) {ofm_vintage <- 2022} else {ofm_vintage <- yr}}
    
    q <- paste0("SELECT parcel_dim_id, estimate_year, total_pop from ofm.parcelized_saep_facts WHERE ofm_vintage = ", ofm_vintage, " AND estimate_year = ", yr, "")
    p <- get_query(sql = q)
    
    # Parcel Dimensions
    if (yr >=2018) {parcel_yr <- 2018} else {parcel_yr <- 2014}
    print(str_glue("Loading {parcel_yr} parcel dimensions from Elmer"))
    q <- paste0("SELECT parcel_dim_id, parcel_id, x_coord_state_plane, y_coord_state_plane, tract_geoid10, tract_geoid20 from small_areas.parcel_dim WHERE base_year = ", parcel_yr, " ")
    d <- get_query(sql = q)
    
    # Add Tract GEOIDs to Parcels and Final Cleanup
    print(str_glue("Final Cleanup of {y} parcelized estimates of total population"))
    p <- left_join(p, d, by="parcel_dim_id") |>
      mutate(data_year = y) |>
      select("parcel_id", x = "x_coord_state_plane", y = "y_coord_state_plane", tract10 = "tract_geoid10", tract20 = "tract_geoid20", population = "total_pop", ofm_year = "estimate_year", "data_year")

    # Append Parcel Data
    if(is.null(parcel_data)) {parcel_data <- p} else {parcel_data <- bind_rows(parcel_data, p)}
    rm(p, q, d)
  }
  
  print(str_glue("Selecting Tract 2010 or 2020 as tract_geoid based on data year."))
  parcel_data <- parcel_data |>
    mutate(tract_geoid = case_when(
      data_year < 2020 ~ tract10,
      data_year >= 2020 ~ tract20)) |>
    mutate(tract_geoid = as.character(tract_geoid))
  
  print(str_glue("Calculating parcel population by EFA."))
  parcel_data <- left_join(parcel_data, tract_efa_data, by=c("tract_geoid", "data_year")) |>
    mutate(poc = round(poc * population, 4)) |>
    mutate(poverty = round(poverty * population, 4)) |>
    mutate(lep = round(lep * population, 4)) |>
    mutate(youth = round(youth * population, 4)) |>
    mutate(older = round(older * population, 4)) |>
    mutate(disability = round(disability * population, 4)) |>
    drop_na() |>
    select("parcel_id", "x", "y", tract = "tract_geoid", gtfs_year = "data_year", "ofm_year", "census_year", "population", "poc", pov = "poverty", "lep", yth = "youth", old = "older", dis = "disability")
  
  saveRDS(parcel_data, "data/parcel_data.rds")

} else {
  print(str_glue("Loading Parcel Population Data from stored results."))
  parcel_data <- readRDS("data/parcel_data.rds")
  
}

# Population Estimates near Transit by Mode ------------------------------------
if(generate_transit_population == "yes") {
  
  transit_buffer_data <- NULL
  for(y in gtfs_years) {
    
    hct_qtr <- calculate_transit_buffer_data(yr = y, modes = hct_modes, mode_name = "High-Capacity Transit", buffer_dist = 0.25)
    bus_qtr <- calculate_transit_buffer_data(yr = y, modes = bus_modes, mode_name = "Local & Regional Bus", buffer_dist = 0.25)
    rail_qtr <- calculate_transit_buffer_data(yr = y, modes = rail_modes, mode_name = "Light Rail & Streetcar", buffer_dist = 0.25)
    ferry_qtr <- calculate_transit_buffer_data(yr = y, modes = ferry_modes, mode_name = "Ferries", buffer_dist = 0.25)
    transit_qtr <- calculate_transit_buffer_data(yr = y, modes = transit_modes, mode_name = "All Transit", buffer_dist = 0.25)
    
    hct_hlf <- calculate_transit_buffer_data(yr = y, modes = hct_modes, mode_name = "High-Capacity Transit", buffer_dist = 0.50)
    bus_hlf <- calculate_transit_buffer_data(yr = y, modes = bus_modes, mode_name = "Local & Regional Bus", buffer_dist = 0.50)
    rail_hlf <- calculate_transit_buffer_data(yr = y, modes = rail_modes, mode_name = "Light Rail & Streetcar", buffer_dist = 0.50)
    ferry_hlf <- calculate_transit_buffer_data(yr = y, modes = ferry_modes, mode_name = "Ferries", buffer_dist = 0.50)
    transit_hlf <- calculate_transit_buffer_data(yr = y, modes = transit_modes, mode_name = "All Transit", buffer_dist = 0.50)
    
    d <- bind_rows(hct_qtr, bus_qtr, rail_qtr, ferry_qtr, transit_qtr, hct_hlf, bus_hlf, rail_hlf, ferry_hlf, transit_hlf)
    if(is.null(transit_buffer_data)) {transit_buffer_data <- d} else {transit_buffer_data <- bind_rows(transit_buffer_data, d)}
    rm(hct_qtr, bus_qtr, rail_qtr, ferry_qtr, transit_qtr, hct_hlf, bus_hlf, rail_hlf, ferry_hlf, transit_hlf, d)
  }
  
  saveRDS(transit_buffer_data, "data/transit_buffer_data.rds")
  
} else {
  
  print(str_glue("Loading Transit Buffer Population Data from stored results."))
  transit_buffer_data <- readRDS("data/transit_buffer_data.rds")
  
}

# Population Estimates near Transit by Trips ------------------------------------
if(generate_transit_trip_population == "yes") {
  
  transit_trip_data <- NULL
  for(y in gtfs_years) {
    
    trips_1_qtr <- calculate_transit_trip_data(yr = y, num_trips = 14, trips_name = "1 Trip per hour", buffer_dist = 0.25)
    trips_2_qtr <- calculate_transit_trip_data(yr = y, num_trips = 28, trips_name = "2 Trips per hour", buffer_dist = 0.25)
    trips_4_qtr <- calculate_transit_trip_data(yr = y, num_trips = 56, trips_name = "4 trips per hour", buffer_dist = 0.25)
    trips_8_qtr <- calculate_transit_trip_data(yr = y, num_trips = 112, trips_name = "8 trips per hour", buffer_dist = 0.25)
    trips_12_qtr <- calculate_transit_trip_data(yr = y, num_trips = 168, trips_name = "12 trips per hour", buffer_dist = 0.25)
    trips_20_qtr <- calculate_transit_trip_data(yr = y, num_trips = 280, trips_name = "20 trips per hour", buffer_dist = 0.25)
    
    trips_1_hlf <- calculate_transit_trip_data(yr = y, num_trips = 14, trips_name = "1 Trip per hour", buffer_dist = 0.50)
    trips_2_hlf <- calculate_transit_trip_data(yr = y, num_trips = 28, trips_name = "2 Trips per hour", buffer_dist = 0.50)
    trips_4_hlf <- calculate_transit_trip_data(yr = y, num_trips = 56, trips_name = "4 trips per hour", buffer_dist = 0.50)
    trips_8_hlf <- calculate_transit_trip_data(yr = y, num_trips = 112, trips_name = "8 trips per hour", buffer_dist = 0.50)
    trips_12_hlf <- calculate_transit_trip_data(yr = y, num_trips = 168, trips_name = "12 trips per hour", buffer_dist = 0.50)
    trips_20_hlf <- calculate_transit_trip_data(yr = y, num_trips = 280, trips_name = "20 trips per hour", buffer_dist = 0.50)
    
    d <- bind_rows(trips_1_qtr, trips_2_qtr, trips_4_qtr, trips_8_qtr, trips_12_qtr, trips_20_qtr,
                   trips_1_hlf, trips_2_hlf, trips_4_hlf, trips_8_hlf, trips_12_hlf, trips_20_hlf)
    
    if(is.null(transit_trip_data)) {transit_trip_data <- d} else {transit_trip_data <- bind_rows(transit_trip_data, d)}
    rm(trips_1_qtr, trips_2_qtr, trips_4_qtr, trips_8_qtr, trips_12_qtr, trips_20_qtr, trips_1_hlf, trips_2_hlf, trips_4_hlf, trips_8_hlf, trips_12_hlf, trips_20_hlf, d)
  }
  
  saveRDS(transit_trip_data, "data/transit_trip_data.rds")
  
} else {
  
  print(str_glue("Loading Transit Trip Population Data from stored results."))
  transit_trip_data <- readRDS("data/transit_trip_data.rds")
  
}

# Transit Layers by Year ------------------------------------
if(generate_transit_layers == "yes") {
  
  transit_layer_data <- NULL
  for(y in gtfs_years) {
    
    lyr <- transit_routes_by_mode(year = y, service_change = gtfs_service)
    if(is.null(transit_layer_data)) {transit_layer_data <- lyr} else {transit_layer_data <- bind_rows(transit_layer_data, lyr)}
    rm(lyr)
  }
  
  saveRDS(transit_layer_data, "data/transit_layer_data.rds")
  
} else {
  
  print(str_glue("Loading Transit Layer Data from stored results."))
  transit_layer_data <- readRDS("data/transit_layer_data.rds")
  
}

# Transit Buffers for Maps ------------------------------------------------
if(generate_transit_buffers == "yes") {
  
  hct_qtr <- create_transit_buffer(modes = hct_modes, mode_name = "High-Capacity Transit", buffer_dist = 0.25)
  bus_qtr <- create_transit_buffer(modes = bus_modes, mode_name = "Local & Regional Bus", buffer_dist = 0.25)
  rail_qtr <- create_transit_buffer(modes = rail_modes, mode_name = "Light Rail & Streetcar", buffer_dist = 0.25)
  ferry_qtr <- create_transit_buffer(modes = ferry_modes, mode_name = "Ferries", buffer_dist = 0.25)
  transit_qtr <- create_transit_buffer(modes = transit_modes, mode_name = "All Transit", buffer_dist = 0.25)
  
  hct_hlf <- create_transit_buffer(modes = hct_modes, mode_name = "High-Capacity Transit", buffer_dist = 0.50)
  bus_hlf <- create_transit_buffer(modes = bus_modes, mode_name = "Local & Regional Bus", buffer_dist = 0.50)
  rail_hlf <- create_transit_buffer(modes = rail_modes, mode_name = "Light Rail & Streetcar", buffer_dist = 0.50)
  ferry_hlf <- create_transit_buffer(modes = ferry_modes, mode_name = "Ferries", buffer_dist = 0.50)
  transit_hlf <- create_transit_buffer(modes = transit_modes, mode_name = "All Transit", buffer_dist = 0.50)
  
  transit_buffers <- bind_rows(hct_qtr, bus_qtr, rail_qtr, ferry_qtr, transit_qtr, hct_hlf, bus_hlf, rail_hlf, ferry_hlf, transit_hlf)
  rm(hct_qtr, bus_qtr, rail_qtr, ferry_qtr, transit_qtr, hct_hlf, bus_hlf, rail_hlf, ferry_hlf, transit_hlf)
  
  saveRDS(transit_buffers, "data/transit_buffers.rds")

} else {
  
  print(str_glue("Loading Transit Buffers from stored results."))
  transit_buffers <- readRDS("data/transit_buffers.rds")
  
}

# Transit Trip Buffers for Maps ------------------------------------------------
if(generate_transit_trip_buffers == "yes") {
  
  trips_1_qtr <- create_transit_trip_buffer(num_trips = 14, trips_name = "1 Trip per hour", buffer_dist = 0.25)
  trips_2_qtr <- create_transit_trip_buffer(num_trips = 28, trips_name = "2 Trips per hour", buffer_dist = 0.25)
  trips_4_qtr <- create_transit_trip_buffer(num_trips = 56, trips_name = "4 trips per hour", buffer_dist = 0.25)
  trips_8_qtr <- create_transit_trip_buffer(num_trips = 112, trips_name = "8 trips per hour", buffer_dist = 0.25)
  trips_12_qtr <- create_transit_trip_buffer(num_trips = 168, trips_name = "12 trips per hour", buffer_dist = 0.25)
  trips_20_qtr <- create_transit_trip_buffer(num_trips = 280, trips_name = "20 trips per hour", buffer_dist = 0.25)

  trips_1_hlf <- create_transit_trip_buffer(num_trips = 14, trips_name = "1 Trip per hour", buffer_dist = 0.50)
  trips_2_hlf <- create_transit_trip_buffer(num_trips = 28, trips_name = "2 Trips per hour", buffer_dist = 0.50)
  trips_4_hlf <- create_transit_trip_buffer(num_trips = 56, trips_name = "4 trips per hour", buffer_dist = 0.50)
  trips_8_hlf <- create_transit_trip_buffer(num_trips = 112, trips_name = "8 trips per hour", buffer_dist = 0.50)
  trips_12_hlf <- create_transit_trip_buffer(num_trips = 168, trips_name = "12 trips per hour", buffer_dist = 0.50)
  trips_20_hlf <- create_transit_trip_buffer(num_trips = 280, trips_name = "20 trips per hour", buffer_dist = 0.50)
  
  transit_trip_buffers <- bind_rows(trips_1_qtr, trips_2_qtr, trips_4_qtr, trips_8_qtr, trips_12_qtr, trips_20_qtr,
                                    trips_1_hlf, trips_2_hlf, trips_4_hlf, trips_8_hlf, trips_12_hlf, trips_20_hlf)
  
  rm(trips_1_qtr, trips_2_qtr, trips_4_qtr, trips_8_qtr, trips_12_qtr, trips_20_qtr, trips_1_hlf, trips_2_hlf, trips_4_hlf, trips_8_hlf, trips_12_hlf, trips_20_hlf)
  
  saveRDS(transit_trip_buffers, "data/transit_trip_buffers.rds")
  
} else {
  
  print(str_glue("Loading Transit Buffers from stored results."))
  transit_trip_buffers <- readRDS("data/transit_trip_buffers.rds")
  
}

# Data for Heat Map -------------------------------------------------------
if(generate_heat_map_buffers == "yes") {
  
  transit_trips <- c(1, 2, 3, 4, 6, 8, 10, 12, 15, 20, 30)
  current_year <- 2024
  
  transit_heat_map_data <- NULL
  for (trips in transit_trips) {
    qtr <- create_transit_trip_buffer(num_trips = trips*14, trips_name = paste0(trips, " per hour"), buffer_dist = 0.25, yrs=current_year)
    hlf <- create_transit_trip_buffer(num_trips = trips*14, trips_name = paste0(trips, " per hour"), buffer_dist = 0.50, yrs=current_year)
    if(is.null(transit_heat_map_data)) {transit_heat_map_data <- bind_rows(qtr, hlf)} else {transit_heat_map_data <- bind_rows(transit_heat_map_data, qtr, hlf)}
    rm(qtr, hlf)
  }
  
  saveRDS(transit_heat_map_data, "data/transit_heat_map_data.rds")
  st_write(transit_heat_map_data, dsn = "data/layers/transit_trips.shp")

}

