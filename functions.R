# Data Processing ---------------------------------------------------------
process_ntd_ridership_data <- function() {
  
  # Location of the most recently downloaded NTD file
  data_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/ntd-data.xlsx"
  
  # Silence the dplyr summarize message
  options(dplyr.summarise.inform = FALSE)
  
  # Passenger Trips, Revenue-Miles and Revenue-Hours tabs
  ntd_tabs <- c("UPT", "VRM", "VRH")
  
  # Figure out which Transit Agencies serve which MPO's
  agency_file <- "data/transit-agency.csv"
  print(str_glue("Figuring out which Transit agencies are in which Metro Area."))
  agencies <- read_csv(agency_file, show_col_types = FALSE) |>
    mutate(NTDID = str_pad(string=NTDID, width=5, pad="0", side=c("left"))) |>
    mutate(UACE = str_pad(string=UACE, width=5, pad="0", side=c("left")))
  
  ntd_ids <- agencies |> select("NTDID") |> distinct() |> pull()
  
  # Lookup for NTD Modes
  mode_file <- "data/ntd-modes.csv"
  ntd_modes <- read_csv(mode_file, show_col_types = FALSE)
  
  # Initial processing of NTD data
  processed <- NULL
  for (areas in ntd_tabs) {
    print(str_glue("Working on {areas} data processing and cleanup."))
    
    # Open file and filter data to only include operators for RTP analysis
    t <- as_tibble(read.xlsx(data_file, sheet = areas, skipEmptyRows = TRUE, startRow = 1, colNames = TRUE)) |>
      mutate(NTD.ID = str_pad(string=NTD.ID, width=5, pad="0", side=c("left"))) |>
      filter(NTD.ID %in% ntd_ids) |> 
      mutate(Mode = case_when(
        Mode == "DR" & TOS == "DO" ~ "DR-DO",
        Mode == "DR" & TOS == "PT" ~ "DR-PT",
        Mode == "DR" & TOS == "TN" ~ "DR-TN",
        Mode == "DR" & TOS == "TX" ~ "DR-TX",
        TRUE ~ Mode)) |> 
      select(-"Legacy.NTD.ID", -contains("Status"), -"Reporter.Type", -"UACE.CD", -"UZA.Name", -"TOS", -"3.Mode") |> 
      pivot_longer(cols = 4:last_col(), names_to = "date", values_to = "estimate", values_drop_na = TRUE) |> 
      mutate(date = my(date))
    
    # Add Detailed Mode Names & Aggregate  
    t <- left_join(t, ntd_modes, by=c("Mode")) |> 
      rename(variable="mode_name") |> 
      select(-"Mode") |>
      group_by(NTD.ID, Agency, date, variable) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble()
    
    # Add Metro Area Name
    n <- agencies |> select("NTDID", "MPO_AREA", "AGENCY_NAME")
    t <- left_join(t, n, by=c("NTD.ID"="NTDID"), relationship = "many-to-many") |>
      select(-"NTD.ID", -"Agency") |>
      rename(grouping="MPO_AREA", geography="AGENCY_NAME") |>
      as_tibble() |>
      mutate(metric=areas) |>
      mutate(metric = case_when(
        metric == "UPT" ~ "Boardings",
        metric == "VRM" ~ "Revenue-Miles",
        metric == "VRH" ~ "Revenue-Hours"))
    
    rm(n)
    
    # Full Year Data
    
    max_yr <- t |> select("date") |> distinct() |> pull() |> max() |> year()
    max_mo <- t |> select("date") |> distinct() |> pull() |> max() |> month()
    
    if (max_mo <12) {
      yr <- max_yr-1
    } else {
      yr <- max_yr
    }
    
    # Trim Data so it only includes full year data and combine
    print(str_glue("Trim {areas} data to only include full year data."))
    full_yr <- t |>
      filter(year(date)<=yr) |>
      mutate(year = year(date)) |>
      group_by(year, variable, grouping, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(date = ymd(paste0(year,"-12-01"))) |>
      select(-"year")
    
    # Metro Areas only need to compare at the total level
    print(str_glue("Summaring Metro Area {areas} full year data."))
    metro_total <-  full_yr |>
      group_by(date, grouping, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography=grouping, geography_type="Metro Areas", variable="All Transit Modes", grouping="Annual")
    
    # PSRC Region by Mode
    print(str_glue("Summaring PSRC Region {areas} full year data by Mode."))
    region_modes <-  full_yr |>
      filter(grouping=="Seattle") |>
      group_by(date, variable, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Region", grouping="Annual")
    
    # PSRC Region Total
    print(str_glue("Summaring PSRC Region {areas} full year data for All Transit."))
    region_total <-  full_yr |>
      filter(grouping=="Seattle") |>
      group_by(date, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Region", grouping="Annual", variable="All Transit Modes")
    
    # PSRC Region by Operator and Mode
    print(str_glue("Summaring PSRC Region {areas} full year data by Transit Operator and Modes."))
    region_operator_modes <-  full_yr |>
      filter(grouping=="Seattle") |>
      mutate(geography_type="Transit Operator", grouping="Annual")
    
    # PSRC Region by Operator
    print(str_glue("Summaring PSRC Region {areas} full year data by Transit Operator for All Transit."))
    region_operator <-  full_yr |>
      filter(grouping=="Seattle") |>
      group_by(date, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography_type="Transit Operator", grouping="Annual", variable="All Transit Modes")
    
    ifelse(is.null(processed), 
           processed <- bind_rows(list(region_operator,region_operator_modes,
                                       region_total,region_modes,
                                       metro_total)), 
           processed <- bind_rows(list(processed,
                                       region_operator,region_operator_modes,
                                       region_total,region_modes,
                                       metro_total)))
    
    rm(region_operator,region_operator_modes,
       region_total,region_modes,
       metro_total, full_yr)
    
    # Year to Date
    
    # Ensure that all data is consistent - find the maximum month for YTD calculations
    max_mo <- t |> select("date") |> distinct() |> pull() |> max() |> month()
    
    # Trim Data so it only includes ytd for maximum month and combine
    print(str_glue("Trim {areas} data to only months for year to date data through month {max_mo}."))
    ytd <- t |>
      filter(month(date)<=max_mo) |>
      mutate(year = year(date)) |>
      group_by(year, variable, grouping, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(date = ymd(paste0(year,"-",max_mo,"-01"))) |>
      select(-"year")
    
    # Metro Areas only need to compare at the total level
    print(str_glue("Summaring Metro Area {areas} year to date data through month {max_mo}."))
    metro_total <-  ytd |>
      group_by(date, grouping, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography=grouping, geography_type="Metro Areas", variable="All Transit Modes", grouping="YTD")
    
    # PSRC Region by Mode
    print(str_glue("Summaring PSRC region {areas} year to date data by mode through month {max_mo}."))
    region_modes <-  ytd |>
      filter(grouping=="Seattle") |>
      group_by(date, variable, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Region", grouping="YTD")
    
    # PSRC Region Total
    print(str_glue("Summaring PSRC region {areas} year to date data by All Transit through month {max_mo}."))
    region_total <-  ytd |>
      filter(grouping=="Seattle") |>
      group_by(date, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Region", grouping="YTD", variable="All Transit Modes")
    
    # PSRC Region by Operator and Mode
    print(str_glue("Summaring PSRC region {areas} year to date data by Operator and Mode through month {max_mo}."))
    region_operator_modes <-  ytd |>
      filter(grouping=="Seattle") |>
      mutate(geography_type="Transit Operator", grouping="YTD")
    
    # PSRC Region by Operator
    print(str_glue("Summaring PSRC region {areas} year to date data by Operator through month {max_mo}."))
    region_operator <-  ytd |>
      filter(grouping=="Seattle") |>
      group_by(date, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography_type="Transit Operator", grouping="YTD", variable="All Transit Modes")
    
    # Add YTD to Annual Data
    processed <- bind_rows(list(processed,
                                region_operator,region_operator_modes,
                                region_total,region_modes,
                                metro_total))
    
    rm(region_operator,region_operator_modes,
       region_total,region_modes,
       metro_total, ytd, t)
    
  }
  
  # Pivot NTD data wide and create new metric: boardings per revenue-hour
  print(str_glue("Calculating Boardings per Revenue Hour andd performing final cleanup."))
  processed_wide <- processed |> 
    pivot_wider(names_from = "metric",
                values_from = "estimate") |> 
    mutate(`Boardings-per-Hour` = ifelse(`Revenue-Hours` > 0,
                                         round(`Boardings` / `Revenue-Hours`, 2), NA))
  
  # Pivot NTD data back to long form for app use
  processed <- processed_wide |> 
    pivot_longer(cols = c("Boardings",
                          "Revenue-Miles",
                          "Revenue-Hours",
                          "Boardings-per-Hour"),
                 names_to = "metric",
                 values_to = "estimate")
  
  processed <- processed |> 
    mutate(year = as.character(year(date))) |>
    mutate(concept = "Transit Agency Service Data") |>
    select("year", "date", "geography", "geography_type", "variable", "grouping", "metric", "concept", "estimate") |>
    drop_na() |>
    filter(estimate >0)
  
  print(str_glue("All done."))
  
  return(processed)
  
}

process_ntd_operating_expense_data <- function(start_yr = first_yr) {
  
  # Location of the most recently downloaded NTD file
  data_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/ntd-ts21-service-data-and-operating-expenses-time-series-by-mode.xlsx"
  
  # Silence the dplyr summarize message
  options(dplyr.summarise.inform = FALSE)

  # Figure out which Transit Agencies serve which MPO's
  agency_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/transit-agency.csv"
  print(str_glue("Figuring out which Transit agencies are in the Seattle Mero Area."))
  agencies <- read_csv(agency_file, show_col_types = FALSE) |>
    filter(MPO_AREA == "Seattle") |>
    mutate(NTDID = str_pad(string=NTDID, width=5, pad="0", side=c("left"))) |>
    select("NTDID", geography = "AGENCY_NAME")
  
  ntd_ids <- agencies |> select("NTDID") |> distinct() |> pull()
  
  # Lookup for NTD Modes
  mode_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/ntd-modes.csv"
  modes <- read_csv(mode_file, show_col_types = FALSE)
  
  # Expense & Performance Tabs tabs
  tabs <- c("OpExp Total", "OpExp VO", "OpExp VM", "OpExp NVM", "OpExp GA", "FARES","VRM", "VRH", "UPT")
  
  # Initial processing of NTD data
  processed <- NULL
  for (areas in tabs) {
    print(str_glue("Working on {areas} data processing and cleanup."))
    
    # Open file and filter data to only include operators for RTP analysis
    t <- as_tibble(read.xlsx(data_file, sheet = areas, skipEmptyRows = TRUE, startRow = 1, colNames = TRUE)) |>
      filter(Agency.Status == "Active" & Reporter.Type == "Full Reporter") |>
      mutate(NTD.ID = str_pad(string=NTD.ID, width=5, pad="0", side=c("left"))) |>
      filter(NTD.ID %in% ntd_ids) |>
      select(-"Legacy.NTD.ID", -contains("Status"), -"Reporter.Type", -"Reporting.Module", -"City", -"State", -contains("UZA"), , -contains("Year"), -"UACE.Code", -"Service") |> 
      pivot_longer(cols = 4:last_col(), names_to = "year", values_to = "estimate", values_drop_na = TRUE) |> 
      mutate(metric = areas, grouping = "Annual", geography_type = "Mode", date = mdy(paste0("12-01-", year)))
    
    # Add Agency Name
    t <- left_join(t, agencies, by=c("NTD.ID"="NTDID"), relationship = "many-to-many") |>
      select("year", "date", "geography", "geography_type", "Mode", "grouping", "metric", "estimate")
    
    # Add Detailed Mode Names & Aggregate  
    t <- left_join(t, modes, by=c("Mode")) |> 
      rename(variable="mode_name") |> 
      select(-"Mode") |>
      group_by(year, date, geography, geography_type, grouping, metric, variable) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      filter(year >= start_yr) |>
      arrange(geography, variable, year)
    
    # Operator Total
    print(str_glue("Summaring Operator {areas} full year data."))
    operator_total <-  t |>
      group_by(year, date, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography_type="Mode", grouping="Annual", variable = "All Transit Modes") |>
      arrange(geography, variable, year)
    
    # PSRC Region by Mode
    print(str_glue("Summaring PSRC Region {areas} full year data by Mode."))
    region_mode <-  t |>
      group_by(year, date, metric, variable) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Mode", grouping="Annual") |>
      arrange(geography, variable, year)
    
    # PSRC Region Total
    print(str_glue("Summaring PSRC Region {areas} full year data."))
    region_total <-  t |>
      group_by(year, date, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Mode", grouping="Annual", variable = "All Transit Modes") |>
      arrange(geography, variable, year)

    ifelse(is.null(processed), 
           processed <- bind_rows(t, operator_total, region_mode, region_total), 
           processed <- bind_rows(processed, t, operator_total, region_mode, region_total))
    
    rm(t, region_mode, region_total, operator_total)
  }
  
  # Calculate total expense per revenue-hour  
  print(str_glue("Calculating Total Expenses per Revenue Hour."))
  e <- processed |> filter(metric == "OpExp Total")
  m <- processed |> filter(metric == "VRH") |> select("year", "geography", "variable", m = "estimate")
  e <- left_join(e, m, by = c("year", "geography", "variable")) |> mutate(estimate = estimate / m) |> select(-"m") |> mutate(metric = "Total Cost per Revenue-Hour")
  processed <- bind_rows(processed, e)
  rm(e, m)
  
  # Calculate total expense per revenue-mile  
  print(str_glue("Calculating Total Expenses per Revenue Mile."))
  e <- processed |> filter(metric == "OpExp Total")
  m <- processed |> filter(metric == "VRM") |> select("year", "geography", "variable", m = "estimate")
  e <- left_join(e, m, by = c("year", "geography", "variable")) |> mutate(estimate = estimate / m) |> select(-"m") |> mutate(metric = "Total Cost per Revenue-Mile")
  processed <- bind_rows(processed, e)
  rm(e, m)
  
  # Calculate total expense per boarding  
  print(str_glue("Calculating Total Expenses per Boardings."))
  e <- processed |> filter(metric == "OpExp Total")
  m <- processed |> filter(metric == "UPT") |> select("year", "geography", "variable", m = "estimate")
  e <- left_join(e, m, by = c("year", "geography", "variable")) |> mutate(estimate = estimate / m) |> select(-"m") |> mutate(metric = "Total Cost per Boarding")
  processed <- bind_rows(processed, e)
  rm(e, m)
  
  # Calculate fare per boarding  
  print(str_glue("Calculating Average Fare per Boardings."))
  e <- processed |> filter(metric == "FARES")
  m <- processed |> filter(metric == "UPT") |> select("year", "geography", "variable", m = "estimate")
  e <- left_join(e, m, by = c("year", "geography", "variable")) |> mutate(estimate = estimate / m) |> select(-"m") |> mutate(metric = "Average Fare per Boarding")
  processed <- bind_rows(processed, e)
  rm(e, m)
  
  # Final Cleanup  
  print(str_glue("Final Data Processing and Cleanup."))
  processed <- processed |> 
    mutate(year = as.character(year(date))) |>
    mutate(concept = "Transit Agency Operating Expenses") |>
    select("year", "date", "geography", "geography_type", "variable", "grouping", "metric", "concept", "estimate") |>
    drop_na() |>
    filter(!(is.infinite(estimate))) |>
    filter(estimate >0) |>
    mutate(metric = str_replace_all(metric, "FARES", "Fare Revenues")) |>
    mutate(metric = str_replace_all(metric, "OpExp Total", "Total Operating Expenses")) |>
    mutate(metric = str_replace_all(metric, "OpExp VO", "Vehicle Operations Expenses")) |>
    mutate(metric = str_replace_all(metric, "OpExp VM", "Vehicle Maintenance Expenses")) |>
    mutate(metric = str_replace_all(metric, "OpExp NVM", "Non-Vehicle Maintenance Expenses")) |>
    mutate(metric = str_replace_all(metric, "OpExp GA", "General Administration Expenses")) |>
    filter(metric != "VRM") |>
    filter(metric != "VRH") |>
    filter(metric != "UPT") 
    
  print(str_glue("All done."))
  
  return(processed)
  
}

process_ntd_capital_expenditures <- function(start_yr = first_yr) {
  
  # Location of the most recently downloaded NTD file
  data_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/ntd-ts31-capital-expenditures-time-series.xlsx"
  
  # Silence the dplyr summarize message
  options(dplyr.summarise.inform = FALSE)
  
  # Figure out which Transit Agencies serve which MPO's
  agency_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/transit-agency.csv"
  print(str_glue("Figuring out which Transit agencies are in the Seattle Mero Area."))
  agencies <- read_csv(agency_file, show_col_types = FALSE) |>
    filter(MPO_AREA == "Seattle") |>
    mutate(NTDID = str_pad(string=NTDID, width=5, pad="0", side=c("left"))) |>
    select("NTDID", geography = "AGENCY_NAME")
  
  ntd_ids <- agencies |> select("NTDID") |> distinct() |> pull()
  
  # Lookup for NTD Modes
  mode_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/ntd-modes.csv"
  modes <- read_csv(mode_file, show_col_types = FALSE)
  
  # Expense & Performance Tabs tabs
  tabs <- c("Total", "Rolling Stock", "Facilities", "Other")
  
  # Initial processing of data
  processed <- NULL
  for (areas in tabs) {
    print(str_glue("Working on {areas} Capital Expenditures data processing and cleanup."))
    
    # Open file and filter data to only include operators for RTP analysis
    t <- as_tibble(read.xlsx(data_file, sheet = areas, skipEmptyRows = TRUE, startRow = 1, colNames = TRUE)) |>
      filter(Agency.Status == "Active" & Reporter.Type == "Full Reporter") |>
      mutate(NTD.ID = str_pad(string=NTD.ID, width=5, pad="0", side=c("left"))) |>
      filter(NTD.ID %in% ntd_ids) |>
      select(-"Legacy.NTD.ID", -contains("Status"), -"Reporter.Type", -"Reporting.Module", -"City", -"State", -contains("UZA"), , -contains("Year"), -"UACE.Code") |> 
      pivot_longer(cols = 4:last_col(), names_to = "year", values_to = "estimate", values_drop_na = TRUE) |> 
      mutate(metric = areas, grouping = "Annual", geography_type = "Mode", date = mdy(paste0("12-01-", year)))
    
    # Add Agency Name
    t <- left_join(t, agencies, by=c("NTD.ID"="NTDID"), relationship = "many-to-many") |>
      select("year", "date", "geography", "geography_type", "Mode", "grouping", "metric", "estimate")
    
    # Add Detailed Mode Names & Aggregate  
    t <- left_join(t, modes, by=c("Mode")) |> 
      rename(variable="mode_name") |> 
      select(-"Mode") |>
      group_by(year, date, geography, geography_type, grouping, metric, variable) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      filter(year >= start_yr) |>
      arrange(geography, variable, year)
    
    # Operator Total
    print(str_glue("Summaring Operator {areas} full year data."))
    operator_total <-  t |>
      group_by(year, date, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography_type="Mode", grouping="Annual", variable = "All Transit Modes") |>
      arrange(geography, variable, year)
    
    # PSRC Region by Mode
    print(str_glue("Summaring PSRC Region {areas} full year data by Mode."))
    region_mode <-  t |>
      group_by(year, date, metric, variable) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Mode", grouping="Annual") |>
      arrange(geography, variable, year)
    
    # PSRC Region Total
    print(str_glue("Summaring PSRC Region {areas} full year data."))
    region_total <-  t |>
      group_by(year, date, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Mode", grouping="Annual", variable = "All Transit Modes") |>
      arrange(geography, variable, year)
    
    ifelse(is.null(processed), 
           processed <- bind_rows(t, operator_total, region_mode, region_total), 
           processed <- bind_rows(processed, t, operator_total, region_mode, region_total))
    
    rm(t, region_mode, region_total, operator_total)
  }
  
  # Final Cleanup  
  print(str_glue("Final Data Processing and Cleanup."))
  processed <- processed |> 
    mutate(year = as.character(year(date))) |>
    mutate(concept = "Transit Agency Capital Expenditures") |>
    select("year", "date", "geography", "geography_type", "variable", "grouping", "metric", "concept", "estimate") |>
    drop_na() |>
    filter(estimate >0) |>
    mutate(metric = str_replace_all(metric, "Total", "Total Capital Funds")) |>
    mutate(metric = str_replace_all(metric, "Rolling Stock", "Funds spent on Rolling Stock")) |>
    mutate(metric = str_replace_all(metric, "Facilities", "Funds spent on Facilities")) |>
    mutate(metric = str_replace_all(metric, "Other", "Funds spent on other capital projects")) |>
    mutate(geography = factor(geography, levels = geography_order)) |>
    arrange(geography, variable, year)
  
  print(str_glue("All done."))
  
  return(processed)
  
}

process_ntd_operating_and_capital_funding <- function(start_yr = first_yr) {
  
  # Location of the most recently downloaded NTD file
  data_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/ntd-ts12-operating-and-capital-funding-time-series.xlsx"
  
  # Silence the dplyr summarize message
  options(dplyr.summarise.inform = FALSE)
  
  # Figure out which Transit Agencies serve which MPO's
  agency_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/transit-agency.csv"
  print(str_glue("Figuring out which Transit agencies are in the Seattle Mero Area."))
  agencies <- read_csv(agency_file, show_col_types = FALSE) |>
    filter(MPO_AREA == "Seattle") |>
    mutate(NTDID = str_pad(string=NTDID, width=5, pad="0", side=c("left"))) |>
    select("NTDID", geography = "AGENCY_NAME")
  
  ntd_ids <- agencies |> select("NTDID") |> distinct() |> pull()
  
  # Expense & Performance Tabs tabs
  tabs <- c("Operating Total", "Operating Federal", "Operating State", "Operating Local", "Operating Other", "Capital Total", "Capital Federal", "Capital State", "Capital Local", "Capital Other")
  
  # Initial processing of data
  processed <- NULL
  for (areas in tabs) {
    print(str_glue("Working on {areas} Capital & Operating Funding data processing and cleanup."))
    
    # Open file and filter data to only include operators for RTP analysis
    t <- as_tibble(read.xlsx(data_file, sheet = areas, skipEmptyRows = TRUE, startRow = 1, colNames = TRUE)) |>
      filter(Agency.Status == "Active" & Reporter.Type == "Full Reporter") |>
      mutate(NTD.ID = str_pad(string=NTD.ID, width=5, pad="0", side=c("left"))) |>
      filter(NTD.ID %in% ntd_ids) |>
      select(-"Legacy.NTD.ID", -contains("Status"), -"Reporter.Type", -"Reporting.Module", -"City", -"State", -contains("UZA"), , -contains("Year"), -"UACE.Code") |> 
      pivot_longer(cols = 3:last_col(), names_to = "year", values_to = "estimate", values_drop_na = TRUE) |> 
      mutate(metric = areas, grouping = "Annual", geography_type = "Operator", variable  ="All Transit Modes", date = mdy(paste0("12-01-", year)))
    
    # Add Agency Name
    t <- left_join(t, agencies, by=c("NTD.ID"="NTDID"), relationship = "many-to-many") |>
      select("year", "date", "geography", "geography_type", "grouping", "variable", "metric", "estimate") |>
      filter(year >= start_yr) |>
      arrange(geography, variable, year)
    
    # PSRC Region Total
    print(str_glue("Summaring PSRC Region {areas} full year data."))
    region_total <-  t |>
      group_by(year, date, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Operator", grouping="Annual", variable = "All Transit Modes") |>
      arrange(geography, variable, year)
    
    ifelse(is.null(processed), 
           processed <- bind_rows(t, region_total), 
           processed <- bind_rows(processed, t, region_total))
    
    rm(t, region_total)
  }
  
  # Create a Total Funding
  total_funds <- processed |>
    filter(metric == "Operating Total" | metric == "Capital Total") |>
    group_by(year, date, geography) |>
    summarise(estimate = sum(estimate)) |>
    as_tibble() |>
    mutate(geography_type = "Operator", grouping = "Annual", variable = "All Transit Modes", metric = "Total Operating & Capital Funds") |>
    arrange(geography, variable, year)
  
  # Final Cleanup  
  print(str_glue("Final Data Processing and Cleanup."))
  processed <- bind_rows(processed, total_funds) |> 
    mutate(year = as.character(year(date))) |>
    mutate(concept = "Transit Agency Operating & Capital Funds") |>
    select("year", "date", "geography", "geography_type", "variable", "grouping", "metric", "concept", "estimate") |>
    drop_na() |>
    filter(estimate >0) |>
    mutate(metric = str_replace_all(metric, "Operating Total", "Total Operating Funds")) |>
    mutate(metric = str_replace_all(metric, "Operating Federal", "Federal Funds used for Operations")) |>
    mutate(metric = str_replace_all(metric, "Operating State", "State Funds used for Operations")) |>
    mutate(metric = str_replace_all(metric, "Operating Local", "Local Funds used for Operations")) |>
    mutate(metric = str_replace_all(metric, "Operating Other", "Other Funds used for Operations")) |>
    mutate(metric = str_replace_all(metric, "Capital Total", "Total Capital Funds")) |>
    mutate(metric = str_replace_all(metric, "Capital Federal", "Federal Funds used for Capital Improvements")) |>
    mutate(metric = str_replace_all(metric, "Capital State", "State Funds used for Capital Improvements")) |>
    mutate(metric = str_replace_all(metric, "Capital Local", "Local Funds used for Capital Improvements")) |>
    mutate(metric = str_replace_all(metric, "Capital Other", "Other Funds used for Capital Improvements")) |>
    mutate(geography = factor(geography, levels = geography_order)) |>
    arrange(geography, variable, year)
  
  print(str_glue("All done."))
  
  return(processed)
  
}

transit_stops_by_mode <- function(year, service_change) {
  
  hct_file <- "data/hct_ids.csv"
  hct <- read_csv(hct_file, show_col_types = FALSE) 
  
  if (tolower(service_change)=="spring") {data_month = "05"} else (data_month = "10")
  
  options(dplyr.summarise.inform = FALSE)
  gtfs_file <- paste0("C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/gtfs/",tolower(service_change),"/",as.character(year),".zip")
  
  # Open Regional GTFS File and load into memory
  print(str_glue("Opening the {service_change} {year} GTFS archive."))
  gtfs <- read_gtfs(path=gtfs_file, files = c("trips","stops","stop_times", "routes", "shapes"))
  
  # Load Stops
  print(str_glue("Getting the {service_change} {year} stops into a tibble." ))
  stops <- as_tibble(gtfs$stops) |> 
    mutate(stop_id = str_to_lower(stop_id)) |>
    select("stop_id", "stop_name", "stop_lat", "stop_lon")
  
  # Load Routes, add HCT modes and update names and agencies
  print(str_glue("Getting the {service_change} {year} routes into a tibble." ))
  routes <- as_tibble(gtfs$routes) |> 
    mutate(route_id = str_to_lower(route_id)) |>
    select("route_id", "agency_id","route_short_name", "route_long_name", "route_type")
  
  print(str_glue("Adding High-Capacity Transit codes to the {service_change} {year} routes"))
  routes <- left_join(routes, hct, by="route_id") |>
    mutate(type_code = case_when(
      is.na(type_code) ~ route_type,
      !(is.na(type_code)) ~ type_code)) |>
    mutate(route_name = case_when(
      is.na(route_name) ~ route_short_name,
      !(is.na(route_name)) ~ route_name)) |>
    mutate(type_name = case_when(
      is.na(type_name) ~ "Bus",
      !(is.na(type_name)) ~ type_name)) |>
    mutate(agency_name = case_when(
      !(is.na(agency_name)) ~ agency_name,
      is.na(agency_name) & str_detect(route_id, "ct") ~ "Community Transit",
      is.na(agency_name) & str_detect(route_id, "et") ~ "Everett Transit",
      is.na(agency_name) & str_detect(route_id, "kc") ~ "King County Metro",
      is.na(agency_name) & str_detect(route_id, "kt") ~ "Kitsap Transit",
      is.na(agency_name) & str_detect(route_id, "pt") ~ "Pierce Transit",
      is.na(agency_name) & str_detect(route_id, "st") ~ "Sound Transit")) |>
    select("route_id", "route_name", "type_name", "type_code", "agency_name")
  
  # Trips are used to get route id onto stop times
  print(str_glue("Getting the {service_change} {year} trips into a tibble to add route ID to stop times." ))
  trips <- as_tibble(gtfs$trips) |> 
    mutate(route_id = str_to_lower(route_id)) |>
    select("trip_id", "route_id")
  
  trips <- left_join(trips, routes, by=c("route_id"))
  
  # Clean Up Stop Times to get routes and mode by stops served
  print(str_glue("Getting the {service_change} {year} stop times into a tibble to add route information." ))
  stoptimes <- as_tibble(gtfs$stop_times) |>
    mutate(stop_id = str_to_lower(stop_id)) |>
    select("trip_id", "stop_id")
  
  # Determine number of trips per hour by stop
  print(str_glue("Getting the {service_change} {year} number of transit trips by stop a tibble." ))
  stoptrips <- as_tibble(gtfs$stop_times) |>
    mutate(stop_id = str_to_lower(stop_id)) |>
    select("stop_id", "arrival_time") |>
    arrange(stop_id, arrival_time) |>
    mutate(hour = hour(arrival_time), daily_trips = 1) |>
    filter(hour >= 4 & hour <= 22) |>
    group_by(stop_id) |>
    summarise(daily_trips = sum(daily_trips)) |>
    as_tibble()
  
  # Get Mode and agency from trips to stops
  print(str_glue("Getting unique stop list by modes for the {service_change} {year}." ))
  stops_by_mode <- left_join(stoptimes, trips, by=c("trip_id")) |>
    select("stop_id", "type_code", "type_name", "agency_name") |>
    distinct()
  
  print(str_glue("Adding daily trips by stop"))
  stops_by_mode <- left_join(stops_by_mode, stoptrips, by=c("stop_id"))
  
  stops_by_mode <- left_join(stops_by_mode, stops, by=c("stop_id")) |>
    mutate(date=mdy(paste0(data_month,"-01-",year)))
  
  print(str_glue("All Done."))
  
  return(stops_by_mode)
  
}

transit_routes_by_mode <- function(year, service_change) {
  
  hct_file <- "data/hct_ids.csv"
  hct <- read_csv(hct_file, show_col_types = FALSE) 
  
  if (tolower(service_change)=="spring") {data_month = "05"} else (data_month = "10")
  
  options(dplyr.summarise.inform = FALSE)
  gtfs_file <- paste0("C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/gtfs/",tolower(service_change),"/",as.character(year),".zip")
  
  # Open Regional GTFS File and load into memory
  print(str_glue("Opening the {service_change} {year} GTFS archive."))
  gtfs <- read_gtfs(path=gtfs_file, files = c("trips","stops","stop_times", "routes", "shapes"))
  
  # Load Routes, add HCT modes and update names and agencies
  print(str_glue("Getting the {service_change} {year} routes into a tibble." ))
  routes <- as_tibble(gtfs$routes) |> 
    mutate(route_id = str_to_lower(route_id)) |>
    select("route_id", "agency_id","route_short_name", "route_long_name", "route_type")
  
  print(str_glue("Adding High-Capacity Transit codes to the {service_change} {year} routes"))
  routes <- left_join(routes, hct, by="route_id") |>
    mutate(type_code = case_when(
      is.na(type_code) ~ route_type,
      !(is.na(type_code)) ~ type_code)) |>
    mutate(route_name = case_when(
      is.na(route_name) ~ route_short_name,
      !(is.na(route_name)) ~ route_name)) |>
    mutate(type_name = case_when(
      is.na(type_name) ~ "Bus",
      !(is.na(type_name)) ~ type_name)) |>
    mutate(agency_name = case_when(
      !(is.na(agency_name)) ~ agency_name,
      is.na(agency_name) & str_detect(route_id, "ct") ~ "Community Transit",
      is.na(agency_name) & str_detect(route_id, "et") ~ "Everett Transit",
      is.na(agency_name) & str_detect(route_id, "kc") ~ "King County Metro",
      is.na(agency_name) & str_detect(route_id, "kt") ~ "Kitsap Transit",
      is.na(agency_name) & str_detect(route_id, "pt") ~ "Pierce Transit",
      is.na(agency_name) & str_detect(route_id, "st") ~ "Sound Transit")) |>
    select("route_id", "route_name", "type_name", "type_code", "agency_name")
  
  # Load Route Shapes to get Mode information on layers
  route_lyr <- shapes_as_sf(gtfs$shapes)
  
  # Trips are used to get route id onto shapes
  print(str_glue("Getting the {service_change} {year} trips into a tibble to add route ID to stop times." ))
  trips <- as_tibble(gtfs$trips) |> 
    mutate(route_id = str_to_lower(route_id)) |>
    select("route_id", "shape_id") |>
    distinct()
  
  route_lyr <- left_join(route_lyr, trips, by=c("shape_id"))
  
  # Get Mode and agency from routes to shapes
  print(str_glue("Getting route details onto shapes for the {service_change} {year}." ))
  route_lyr <- left_join(route_lyr, routes, by=c("route_id")) |> mutate(date=mdy(paste0(data_month,"-01-",year)))
  
  print(str_glue("All Done."))
  
  return(route_lyr)
  
}

acs_equity_shares <- function(year, census_metric) {
  
  # Determine Table Name, variables and labels from Variable Lookup
  print(str_glue("Loading the {census_metric} variable and label lookups."))
  lookup <- read_csv(paste0("data/census-variable-lookups.csv"), show_col_types = FALSE) |> 
    filter(metric == census_metric) |> 
    mutate(variable = paste0(table, "_", str_pad(variable, width=3, side = 'left', pad="0")))
  census_tbl <- lookup |> select("table") |> distinct() |> pull()
  census_var <- lookup |> select("variable") |> distinct() |> pull()
  census_lbl <- lookup |> select("variable", "metric", "simple_label")
  census_ord <- lookup |> arrange(order) |> select("simple_label") |> distinct() |> pull()
  
  # Use latest Census Year in Analysis year is past 
  if(year > latest_census) {y <- latest_census} else {y <- year}
  
  # Get Data by Census Block Group
  print(str_glue("Downloading {census_metric} data from {y}-ACS 5yr table {census_tbl} for tracts in the region."))
  pop <- get_acs_recs(geography="tract", table.names = census_tbl, years = y, acs.type = 'acs5') |> filter(variable %in% census_var)
  
  # Clean Up labels and group data by labels
  efa_column <- census_ord[census_ord != "Total"]
  print(str_glue("Cleaning up labels and grouping {census_metric} data from {y}-ACS 5yr table {census_tbl} for tracts in the region."))
  pop <- left_join(pop, census_lbl, by=c("variable")) |>
    select(tract="GEOID", "estimate", "moe", "year", "metric", grouping="simple_label") |>
    group_by(tract, year, metric, grouping) |>
    summarise(estimate = sum(estimate)) |>
    as_tibble() |>
    pivot_wider(names_from = grouping, values_from = estimate) |>
    mutate(!!census_metric:= .data[[efa_column]] / Total) |>
    mutate(!!census_metric:= replace_na(.data[[census_metric]], 0)) |>
    select("tract", census_year="year", all_of(census_metric)) |>
    mutate(data_year = year)
  
  return(pop)
  
}

calculate_transit_buffer_data <- function(stops=transit_stops, parcels=parcel_data, yr, modes, mode_name, buffer_dist) {
  
  print(str_glue("Creating {mode_name} stop buffer for {yr} {mode_name} stations."))
  s <- stops |> 
    filter(year(date) == yr) |> 
    filter(type_name %in% modes) |>
    st_as_sf(coords = c("stop_lon", "stop_lat"), crs=wgs84) |>
    st_transform(spn) |>
    st_buffer(dist = buffer_dist * 5280) |>
    mutate(stop_buffer = mode_name) |>
    select("stop_buffer")
  
  print(str_glue("Creating Parcel point layer for {yr} to intersect with {mode_name} stations"))
  p <- parcels |>
    filter(gtfs_year == yr) |>
    select(-"tract", -"gtfs_year", -"ofm_year", -"census_year") |>
    st_as_sf(coords = c("x", "y"), crs=spn) |>
    st_transform(spn)
  
  print(str_glue("Intersecting parcel point data with {mode_name} stop buffer for {yr}."))
  m <- st_intersection(p, s) |>
    st_drop_geometry() |>
    distinct() |>
    mutate(gtfs_year = yr) |>
    group_by(gtfs_year) |>
    summarise(population = round(sum(population),-2), 
              poc = round(sum(poc),-2), pov = round(sum(pov),-2), yth = round(sum(yth),-2), 
              old = round(sum(old),-2), lep = round(sum(lep),-2), dis = round(sum(dis), -2)) |>
    as_tibble()
  
  print(str_glue("Calculating regionwide totals for {yr}"))
  r <- parcels |> 
    filter(gtfs_year == yr) |>
    group_by(gtfs_year) |>   
    summarise(region_population = round(sum(population),-2), 
              region_poc = round(sum(poc),-2), region_pov = round(sum(pov),-2), region_yth = round(sum(yth),-2), 
              region_old = round(sum(old),-2), region_lep = round(sum(lep),-2), region_dis = round(sum(dis), -2)) |>
    as_tibble()
  
  print(str_glue("Calculating shares of people with {mode_name} access for {yr}"))
  d <- left_join(m, r, by="gtfs_year") |>
    mutate(population_share = population/region_population, 
           poc_share = poc/region_poc, pov_share = pov/region_pov, lep_share = lep/region_lep,
           yth_share = yth/region_yth, old_share = old/region_old, dis_share = dis/region_dis) |>
    select(year = "gtfs_year", "population", "population_share", 
           "poc", "poc_share", "pov", "pov_share", "lep", "lep_share",
           "yth", "yth_share", "old", "old_share", "dis", "dis_share") |>
    mutate(transit_buffer = mode_name, buffer = buffer_dist)
  
  return(d)
  
}

create_transit_buffer <- function(stops=transit_stops, yrs=gtfs_years, modes, mode_name, buffer_dist) {
  
  buffers <- NULL
  for(yr in yrs) {
    
    print(str_glue("Creating {mode_name} stop buffer for {yr} {mode_name} stations."))
    s <- stops |> 
      filter(year(date) == yr) |> 
      filter(type_name %in% modes) |>
      st_as_sf(coords = c("stop_lon", "stop_lat"), crs=wgs84) |>
      st_transform(spn) |>
      st_buffer(dist = buffer_dist * 5280) |>
      mutate(stop_buffer = mode_name, buffer = buffer_dist) |>
      select("stop_buffer", "buffer")
      
    t <- st_union(s)
    
    u <- st_sf(geometry=t) |>
      mutate(stop_buffer = mode_name, year = yr, buffer = buffer_dist) |>
      st_transform(wgs84)
    
    if(is.null(buffers)) {buffers <- u} else {buffers <- rbind(buffers, u)}
    rm(s, t, u)
  }
  
  return(buffers)
}

calculate_transit_trip_data <- function(stops=transit_stops, parcels=parcel_data, yr, num_trips, trips_name, buffer_dist) {
  
  print(str_glue("Creating a {buffer_dist} mile stop buffer for {yr} stations with at least {num_trips} daily transit trips."))
  s <- stops |> 
    filter(year(date) == yr & daily_trips >= num_trips ) |> 
    select("stop_id", "daily_trips", "stop_lat", "stop_lon") |>
    distinct() |>
    st_as_sf(coords = c("stop_lon", "stop_lat"), crs=wgs84) |>
    st_transform(spn) |>
    st_buffer(dist = buffer_dist * 5280) |>
    mutate(stop_buffer = trips_name, buffer = buffer_dist) |>
    select("stop_buffer", "buffer")
  
  print(str_glue("Creating Parcel point layer for {yr} to intersect with stations with at least {num_trips} daily transit trips."))
  p <- parcels |>
    filter(gtfs_year == yr) |>
    select(-"tract", -"gtfs_year", -"ofm_year", -"census_year") |>
    st_as_sf(coords = c("x", "y"), crs=spn) |>
    st_transform(spn)
  
  print(str_glue("Intersecting parcel point data with stations with at least {num_trips} daily transit trips stop buffer for {yr}."))
  m <- st_intersection(p, s) |>
    st_drop_geometry() |>
    distinct() |>
    mutate(gtfs_year = yr) |>
    group_by(gtfs_year) |>
    summarise(population = round(sum(population),-2), 
              poc = round(sum(poc),-2), pov = round(sum(pov),-2), yth = round(sum(yth),-2), 
              old = round(sum(old),-2), lep = round(sum(lep),-2), dis = round(sum(dis), -2)) |>
    as_tibble()
  
  print(str_glue("Calculating regionwide totals for {yr}"))
  r <- parcels |> 
    filter(gtfs_year == yr) |>
    group_by(gtfs_year) |>   
    summarise(region_population = round(sum(population),-2), 
              region_poc = round(sum(poc),-2), region_pov = round(sum(pov),-2), region_yth = round(sum(yth),-2), 
              region_old = round(sum(old),-2), region_lep = round(sum(lep),-2), region_dis = round(sum(dis), -2)) |>
    as_tibble()
  
  print(str_glue("Calculating shares of people with stations with at least {num_trips} daily transit trips stop buffer access for {yr}"))
  d <- left_join(m, r, by="gtfs_year") |>
    mutate(population_share = population/region_population, 
           poc_share = poc/region_poc, pov_share = pov/region_pov, lep_share = lep/region_lep,
           yth_share = yth/region_yth, old_share = old/region_old, dis_share = dis/region_dis) |>
    select(year = "gtfs_year", "population", "population_share", 
           "poc", "poc_share", "pov", "pov_share", "lep", "lep_share",
           "yth", "yth_share", "old", "old_share", "dis", "dis_share") |>
    mutate(transit_buffer = trips_name, buffer = buffer_dist)
  
  return(d)
  
}

create_transit_trip_buffer <- function(stops=transit_stops, yrs=gtfs_years, num_trips, trips_name, buffer_dist) {
  
  #water_bodies <- st_read_elmergeo(layer_name = "largest_waterbodies") |> st_transform(crs = spn)
  
  buffers <- NULL
  for(yr in yrs) {
    
    print(str_glue("Creating a {buffer_dist} mile stop buffer for {yr} stations with at least {num_trips} daily transit trips."))
    s <- stops |> 
      filter(year(date) == yr & daily_trips >= num_trips ) |> 
      select("stop_id", "daily_trips", "stop_lat", "stop_lon") |>
      distinct() |>
      st_as_sf(coords = c("stop_lon", "stop_lat"), crs=wgs84) |>
      st_transform(spn) |>
      st_buffer(dist = buffer_dist * 5280) |>
      mutate(stop_buffer = trips_name, buffer = buffer_dist) |>
      select("stop_buffer", "buffer")
    
    # print(str_glue("Removing water bodies from the buffer for display."))
    # intersection <- st_intersection(s, water_bodies)
    # s <- ms_erase(s, intersection)
    
    t <- st_union(s)
    
    u <- st_sf(geometry=t) |>
      mutate(stop_buffer = trips_name, year = yr, buffer = buffer_dist) |>
      st_transform(wgs84)
    
    if(is.null(buffers)) {buffers <- u} else {buffers <- rbind(buffers, u)}
    rm(s, t, u)
  }
  
  return(buffers)
}
