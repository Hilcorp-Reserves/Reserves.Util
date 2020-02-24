# Reserves_Util Package v0.4.0
#
# This package contains multiple utility function that automate Aries workflow.
#
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

library(RODBC)
library(magrittr)
library(dplyr)
library(stringr)
library(reshape2)
library(lubridate)
options(stringsAsFactors = FALSE)

#' Create a SQL Connection
#'
#' @param server A string.
#' @param database A string.
#' @return The handle containing the database connection
#' @examples
#' get_dbhandle("extsql01", "Aries")
#' @description
#' This function opens a connection to the specified Server and Database parameters passed to it.
get_dbhandle = function(server, database){

    db_handle <- RODBC::odbcDriverConnect(paste0('driver={SQL Server};server=', server, ';database=', database, ';trusted_connection=true'))

  return(db_handle)
}  #Complete

#' Queries Aries Input Settings
#'
#' @param user_input_setting String containing the name of the input settings in Aries.
#' @param server String containing the server name where the Aries database is located.
#' @param database String containing the Aries database name.
#' @param export A bolean.
#' @return Returns a list object containing the As of Date, Base Date, PW table, common lines, and default lines for a specified user input settings.
#' @examples
#' get_Input_Settings(user_input_settings = "HEC0119", server = "extsql01", database = "Aries")
#' @description
#' This function will retrieve the As of Date, Base Date, PW table, common lines, and default lines for the input setting specified by the user.
get_Input_Settings = function(user_input_setting, server, database, export = FALSE){

    tryCatch({
        db_handle = get_dbhandle(server = server, database = database)
        ac_setup = sqlQuery(channel = db_handle, query = paste("SELECT * FROM ariesadmin.AC_SETUP;"))
        ac_setupdata = sqlQuery(channel = db_handle, query = paste("SELECT * FROM ariesadmin.AC_SETUPDATA;"))
        odbcClose(db_handle)

        ac_setup = ac_setup[ac_setup$SETUPNAME == user_input_setting, ]

        frame = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$FRAME & ac_setupdata$SECTYPE == "FRAME"), "LINE"] %>% strsplit(., split = " ")
        pw = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$PW & ac_setupdata$SECTYPE == "PW"), "LINE"] %>% strsplit(., split = " ")
        esc = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$ESC & ac_setupdata$SECTYPE == "ESC"), "LINE"][1] %>% strsplit(., split = " ")
        capital = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$CAPITAL & ac_setupdata$SECTYPE == "CAPITAL"), "LINE"] %>% strsplit(., split = " ")
        corp_tax = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$CORPTAX & ac_setupdata$SECTYPE == "CORPTAX"), "LINE"] %>% strsplit(., split = " ")
        ssroy = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$SSROY & ac_setupdata$SECTYPE == "SSROY"), "LINE"] %>% strsplit(., split = " ")
        special = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$SPECIAL & ac_setupdata$SECTYPE == "SPECIAL"), "LINE"] %>% strsplit(., split = " ")
        com_lines = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$COMLINES & ac_setupdata$SECTYPE == "COMLINES"), ]
        def_lines = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$DEFLINES & ac_setupdata$SECTYPE == "DEFLINES"), ]
        va_rates = ac_setupdata[(ac_setupdata$SECNAME == ac_setup$VARATES & ac_setupdata$SECTYPE == "VARATES"), "LINE"] %>% strsplit(., split = " ")

        as_of_date = frame[[2]][1]
        base_date = frame[[1]][1]
        pw_table = pw[[2]]
        common_lines = data.frame(com_lines[ ,c(1:3)], Section = substr(x = com_lines$LINENUMBER, start = 1, stop = 1), com_lines[ ,c(4:5)])
        default_lines = data.frame(def_lines[ ,c(1:3)], Section = substr(x = def_lines$LINENUMBER, start = 2, stop = 2), def_lines[ ,c(4:5)])
        
        # Export the data to excel file if user sets export = TRUE
        if(export == TRUE){
            openxlsx::write.xlsx(x = list(As_of_Date = as_of_date, Base_Date = base_date, PW_Table = pw_table, Common_Lines = common_lines, Default_Lines = default_lines),
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "AC_Input_Settings ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx")
            )
        }

        return(list(As_of_Date = as_of_date, Base_Date = base_date, PW_Table = pw_table, Common_Lines = common_lines, Default_Lines = default_lines))

    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })


} # Complete

#' Retrieve Aries economic data.
#'
#' @param server A string.
#' @param database A string.
#' @param PROPNUM A String
#' @param scenario_name A string
#' @param view_by A string
#' @param export A bolean.
#' @return A dataframe containing Economic data
#' @examples
#' get_Economics("extsql01", "Aries", "T64EO3SCKW", "FINAL0119_WF", "Scenario")
#' @description
#' This function returns economic lines as a list by section based on the specified Data Source and Scenario Name.
#' PROPNUM is a vector of PROPNUMs to be queried.
#' The View_by parameter determines how the function queries the data:
#'    "All" = all lines,
#'    "Full Scenario" = All qualified lines within desired scenario,
#'    "Scenario" = Highest qualified line specific to scenario
get_Economics = function(server, database, PROPNUM, scenario_name, view_by = "Scenario", export = FALSE){
    tryCatch({
        db_handle = get_dbhandle(server, database)
        scenario = RODBC::sqlQuery(db_handle,
                                   query = paste0("SELECT * FROM Ariesadmin.AC_SCENARIO WHERE Ariesadmin.AC_SCENARIO.SCEN_NAME = '", scenario_name, "';"))
        scenario[] = lapply(scenario, as.character)
        qualifiers = scenario[,c("QUAL0", "QUAL1", "QUAL2", "QUAL3", "QUAL4", "QUAL5", "QUAL6", "QUAL7", "QUAL8", "QUAL9")]
        qualifiers = unique(qualifiers[!is.na(qualifiers)])

        query = paste0("SELECT Ariesadmin.AC_ECONOMIC.PROPNUM,
                       Ariesadmin.AC_ECONOMIC.SECTION,
                       Ariesadmin.AC_ECONOMIC.SEQUENCE,
                       Ariesadmin.AC_ECONOMIC.QUALIFIER,
                       Ariesadmin.AC_ECONOMIC.KEYWORD,
                       Ariesadmin.AC_ECONOMIC.EXPRESSION
                       FROM Ariesadmin.AC_ECONOMIC
                       WHERE Ariesadmin.AC_ECONOMIC.PROPNUM In ('", paste0(PROPNUM, collapse = "','"), "');")

        ac_economic = data.frame(RODBC::sqlQuery(channel = db_handle,
                                      query = query),stringsAsFactors = FALSE)
        RODBC::odbcClose(channel = db_handle)
        ac_economic[] = lapply(ac_economic, as.character)

        if(view_by == "All"){

          #Split AC_ECONOMICS by section and return function
          sect_2_econ = ac_economic[ac_economic$SECTION == 2, ]
          sect_4_econ = ac_economic[ac_economic$SECTION == 4, ]
          sect_5_econ = ac_economic[ac_economic$SECTION == 5, ]
          sect_6_econ = ac_economic[ac_economic$SECTION == 6, ]
          sect_7_econ = ac_economic[ac_economic$SECTION == 7, ]
          sect_8_econ = ac_economic[ac_economic$SECTION == 8, ]
          sect_9_econ = ac_economic[ac_economic$SECTION == 9, ]

        }

        if(view_by == "Full Scenario" | view_by == "Scenario"){
          if(view_by == "Scenario"){
            scenario_section_2 = cbind(data.frame(Rank = seq(1,10,1), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,2], stringsAsFactors = FALSE))

            scenario_section_4 = cbind(data.frame(Rank = seq(1,10,1), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,4], stringsAsFactors = FALSE))

            scenario_section_5 = cbind(data.frame(Rank = seq(1,10,1), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,5], stringsAsFactors = FALSE))

            scenario_section_6 = cbind(data.frame(Rank = seq(1,10,1), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,6], stringsAsFactors = FALSE))

            scenario_section_7 = cbind(data.frame(Rank = seq(1,10,1), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,7], stringsAsFactors = FALSE))

            scenario_section_8 = cbind(data.frame(Rank = seq(1,10,1), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,8], stringsAsFactors = FALSE))

            scenario_section_9 = cbind(data.frame(Rank = seq(1,10,1), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,9], stringsAsFactors = FALSE))
          }
          if(view_by == "Full Scenario"){
            scenario_section_2 = cbind(data.frame(Rank = rep(1,10), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,2], stringsAsFactors = FALSE))

            scenario_section_4 = cbind(data.frame(Rank = rep(1,10), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,4], stringsAsFactors = FALSE))

            scenario_section_5 = cbind(data.frame(Rank = rep(1,10), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,5], stringsAsFactors = FALSE))

            scenario_section_6 = cbind(data.frame(Rank = rep(1,10), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,6], stringsAsFactors = FALSE))

            scenario_section_7 = cbind(data.frame(Rank = rep(1,10), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,7], stringsAsFactors = FALSE))

            scenario_section_8 = cbind(data.frame(Rank = rep(1,10), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,8], stringsAsFactors = FALSE))

            scenario_section_9 = cbind(data.frame(Rank = rep(1,10), stringsAsFactors = FALSE),
                                       data.frame(QUALIFIER= t(scenario)[4:13,9], stringsAsFactors = FALSE))
          }

          sect_2_econ = dplyr::inner_join(ac_economic[ac_economic$SECTION == 2, ],  scenario_section_2 , by = "QUALIFIER")
          if(nrow(sect_2_econ) > 0){sect_2_econ[is.na(sect_2_econ$QUALIFIER), "Rank"] = 100}
          sect_4_econ = dplyr::inner_join(ac_economic[ac_economic$SECTION == 4, ],  scenario_section_4 , by = "QUALIFIER")
          if(nrow(sect_4_econ) > 0){sect_4_econ[is.na(sect_4_econ$QUALIFIER), "Rank"] = 100}
          sect_5_econ = dplyr::inner_join(ac_economic[ac_economic$SECTION == 5, ],  scenario_section_5 , by = "QUALIFIER")
          if(nrow(sect_5_econ) > 0){sect_5_econ[is.na(sect_5_econ$QUALIFIER), "Rank"] = 100}
          sect_6_econ = dplyr::inner_join(ac_economic[ac_economic$SECTION == 6, ],  scenario_section_6 , by = "QUALIFIER")
          if(nrow(sect_6_econ) > 0){sect_6_econ[is.na(sect_6_econ$QUALIFIER), "Rank"] = 100}
          sect_7_econ = dplyr::inner_join(ac_economic[ac_economic$SECTION == 7, ],  scenario_section_7 , by = "QUALIFIER")
          if(nrow(sect_7_econ) > 0){sect_7_econ[is.na(sect_7_econ$QUALIFIER), "Rank"] = 100}
          sect_8_econ = dplyr::inner_join(ac_economic[ac_economic$SECTION == 8, ],  scenario_section_8 , by = "QUALIFIER")
          if(nrow(sect_8_econ) > 0){sect_8_econ[is.na(sect_8_econ$QUALIFIER), "Rank"] = 100}
          sect_9_econ = dplyr::inner_join(ac_economic[ac_economic$SECTION == 9, ],  scenario_section_9 , by = "QUALIFIER")
          if(nrow(sect_9_econ) > 0){sect_9_econ[is.na(sect_9_econ$QUALIFIER), "Rank"] = 100}

          for(i in unique(PROPNUM)){
            if(length(sect_2_econ[sect_2_econ$PROPNUM == i, "Rank"])>0){
              minQualifier = min(sect_2_econ[sect_2_econ$PROPNUM == i, "Rank"])
              sect_2_econ[(sect_2_econ$PROPNUM == i & sect_2_econ$Rank != minQualifier), "PROPNUM"] = "DELETE"
            }
            if(length(sect_4_econ[sect_4_econ$PROPNUM == i, "Rank"])>0){
              minQualifier = min(sect_4_econ[sect_4_econ$PROPNUM == i, "Rank"])
              sect_4_econ[(sect_4_econ$PROPNUM == i & sect_4_econ$Rank != minQualifier), "PROPNUM"] = "DELETE"
            }
            if(length(sect_5_econ[sect_5_econ$PROPNUM == i, "Rank"])>0){
              minQualifier = min(sect_5_econ[sect_5_econ$PROPNUM == i, "Rank"])
              sect_5_econ[(sect_5_econ$PROPNUM == i & sect_5_econ$Rank != minQualifier), "PROPNUM"] = "DELETE"
            }
            if(length(sect_6_econ[sect_6_econ$PROPNUM == i, "Rank"])>0){
              minQualifier = min(sect_6_econ[sect_6_econ$PROPNUM == i, "Rank"])
              sect_6_econ[(sect_6_econ$PROPNUM == i & sect_6_econ$Rank != minQualifier), "PROPNUM"] = "DELETE"
            }
            if(length(sect_7_econ[sect_7_econ$PROPNUM == i, "Rank"])>0){
              minQualifier = min(sect_7_econ[sect_7_econ$PROPNUM == i, "Rank"])
              sect_7_econ[(sect_7_econ$PROPNUM == i & sect_7_econ$Rank != minQualifier), "PROPNUM"] = "DELETE"
            }
            if(length(sect_8_econ[sect_8_econ$PROPNUM == i, "Rank"])>0){
              minQualifier = min(sect_8_econ[sect_8_econ$PROPNUM == i, "Rank"])
              sect_8_econ[(sect_8_econ$PROPNUM == i & sect_8_econ$Rank != minQualifier), "PROPNUM"] = "DELETE"
            }
            if(length(sect_9_econ[sect_9_econ$PROPNUM == i, "Rank"])>0){
              minQualifier = min(sect_9_econ[sect_9_econ$PROPNUM == i, "Rank"])
              sect_9_econ[(sect_9_econ$PROPNUM == i & sect_9_econ$Rank != minQualifier), "PROPNUM"] = "DELETE"
            }
          }
        }

        #Delete excess economic lines that are not part of the scenario or are being filtered out based on user selection.
        sect_2_econ = sect_2_econ[sect_2_econ$PROPNUM != "DELETE", ]
        sect_4_econ = sect_4_econ[sect_4_econ$PROPNUM != "DELETE", ]
        sect_5_econ = sect_5_econ[sect_5_econ$PROPNUM != "DELETE", ]
        sect_6_econ = sect_6_econ[sect_6_econ$PROPNUM != "DELETE", ]
        sect_7_econ = sect_7_econ[sect_7_econ$PROPNUM != "DELETE", ]
        sect_8_econ = sect_8_econ[sect_8_econ$PROPNUM != "DELETE", ]
        sect_9_econ = sect_9_econ[sect_9_econ$PROPNUM != "DELETE", ]

        economics_by_section = list(Section_2 = sect_2_econ,
                                    Section_4 = sect_4_econ,
                                    Section_5 = sect_5_econ,
                                    Section_6 = sect_6_econ,
                                    Section_7 = sect_7_econ,
                                    Section_8 = sect_8_econ,
                                    Section_9 = sect_9_econ)
        
        # Export the data to excel file if user sets export = TRUE
        if(export == TRUE){
            openxlsx::write.xlsx(x = economics_by_section,
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "AC_Economics ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx")
            )
        }
        
        return(economics_by_section)

    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })

}  #Complete

#' Retrieve Enertia Production Data.
#'
#' @param well_comp_codes A string.
#' @param export A bolean.
#' @return A dataframe containing Production data
#' @examples
#' get_Enertia_Production("49.1016.0022.00")
#' @description
#' The following functin will make a connection to the HEC_Repository database located on the Enertia01B Server.
#' It will then take the well_comp_codes vector which contains a sequence of Enertia completion codes to query.
#' The function will then return a dataframe object with all the monthly enertia production data that exists for each
#' Enertia completion code within the well_comp_codes vector.
get_Enertia_Production = function(well_comp_codes, export = FALSE){

  tryCatch({
        #Connect to Datasource
        db_handle = get_dbhandle(server = "enertia01b", database = "HEC_Repository")
        #Compile Query
        query = paste0("SELECT ofm_Lineage.WellCompCode,
                        ofm_Monthly.ProdDate,
                        ofm_Monthly.OilProduction,
                        ofm_Monthly.GasProduction,
                        ofm_Monthly.PProdVol,
                        ofm_Monthly.WaterProduction,
                        ofm_Monthly.TotalGasInjection,
                        ofm_Monthly.WaterInjection,
                        ofm_Monthly.TotalGasProduction
                        FROM ofm_Lineage INNER JOIN ofm_Monthly ON ofm_Lineage.WellCompHID = ofm_Monthly.WellCompHID
                        WHERE ofm_Lineage.WellCompCode IN (", paste0("'", well_comp_codes, collapse ="',") , "');")

        # Query the database, trim any excess white space, convert all columns to character type, adjust producttype to PROPER to prevent issues due to
        # case sensitivty when pvitoting table and convert the volumes to numeric.
        enertia_production = data.frame(RODBC::sqlQuery(db_handle, query))
        enertia_production[] = lapply(enertia_production, trimws)
        enertia_production[] = lapply(enertia_production, as.character)
        enertia_production[,"OilProduction"] = as.numeric(enertia_production[,"OilProduction"])
        enertia_production[,"GasProduction"] = as.numeric(enertia_production[,"GasProduction"])
        enertia_production[,"PProdVol"] = as.numeric(enertia_production[,"PProdVol"])
        enertia_production[,"WaterProduction"] = as.numeric(enertia_production[,"WaterProduction"])
        enertia_production[,"TotalGasInjection"] = as.numeric(enertia_production[,"TotalGasInjection"])
        enertia_production[,"WaterInjection"] = as.numeric(enertia_production[,"WaterInjection"])
        enertia_production[,"TotalGasProduction"] = as.numeric(enertia_production[,"TotalGasProduction"])
        enertia_production$TotalGasInjection = enertia_production$TotalGasProduction - enertia_production$GasProduction

        names(enertia_production) = c("WellCompCode", "Date", "OIL", "GAS", "PPROD", "WATER", "GAS_INJ", "WATER_INJ", "TotalGasProduction")
        # Close ODBC data connection to free up datasource.
        RODBC::odbcClose(channel = db_handle)

        if(!is.null(enertia_production)){
            # Replace all NA values within product columns with a 0
            enertia_production[is.na(enertia_production)] = 0
            # Convert PPROD units from gallons to stb (42gal = 1stb)
            enertia_production$PPROD = enertia_production$PPROD/42
        }
        
        # Export the data to excel file if user sets export = TRUE
        if(export == TRUE){
            openxlsx::write.xlsx(x = enertia_production[, c("WellCompCode", "Date", "OIL", "GAS", "PPROD", "WATER", "GAS_INJ", "WATER_INJ")],
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "Monthly_Prod ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx")
            )
        }
        
      return(enertia_production[, c("WellCompCode", "Date", "OIL", "GAS", "PPROD", "WATER", "GAS_INJ", "WATER_INJ")])

    },
    error = function(cond){

      return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    }

  )


} # Complete

#' Retrieve Daily Enertia Production Data.
#'
#' @param well_comp_codes A string.
#' @param months An integer
#' @param export A bolean.
#' @return A dataframe containing Daily Production data for a specified number of months since the current date
#' @examples
#' get_Enertia_Production("49.1016.0022.00", 5)
#' @description
#' The following functin will make a connection to the ofm database located on the Enertia01B Server.
#' It will then take the well_comp_codes vector which contains a sequence of Enertia completion codes to query.
#' The function will then return a dataframe object with all the enertia daily production data that exists for each
#' Enertia completion code within the well_comp_codes vector for the specified number of months since the current date.
get_Daily_Enertia_Production = function(well_comp_codes, months, export = FALSE){

    tryCatch({
          #Connect to Datasource
          db_handle = get_dbhandle(server = "enertia01b", database = "HEC_Repository")
          t1 = (Sys.Date() - months(months)) %>% floor_date(., unit = "month")
          #Compile Query
          query = paste0("SELECT ofm_Lineage.WellCompCode,
                          ofm_Daily.ProdDate,
                          ofm_Daily.OilProduction,
                          ofm_Daily.GasProduction,
                          ofm_Daily.PProdVolume,
                          ofm_Daily.WaterProduction,
                          ofm_Daily.GasInjection,
                          ofm_Daily.WaterInjection,
                          ofm_Daily.OilSales,
                          ofm_Daily.GasSales
                          FROM ofm_Lineage INNER JOIN ofm_Daily ON ofm_Lineage.WellCompHID = ofm_Daily.WellCompHID
                          WHERE (ofm_Lineage.WellCompCode IN (", paste0("'", well_comp_codes, collapse ="',") , "')
                         AND ofm_Daily.ProdDate >= {d '", t1, "'});")

          # Query the database, trim any excess white space, convert all columns to character type, adjust producttype to PROPER to prevent issues due to
          # case sensitivty when pvitoting table and convert the volumes to numeric.
          enertia_production = data.frame(RODBC::sqlQuery(db_handle, query))
          enertia_production[] = lapply(enertia_production, trimws)
          enertia_production[] = lapply(enertia_production, as.character)
          enertia_production[,"OilProduction"] = as.numeric(enertia_production[,"OilProduction"])
          enertia_production[,"GasProduction"] = as.numeric(enertia_production[,"GasProduction"])
          enertia_production[,"PProdVolume"] = as.numeric(enertia_production[,"PProdVolume"])
          enertia_production[,"WaterProduction"] = as.numeric(enertia_production[,"WaterProduction"])
          enertia_production[,"GasInjection"] = as.numeric(enertia_production[,"GasInjection"])
          enertia_production[,"OilSales"] = as.numeric(enertia_production[,"OilSales"])
          enertia_production[,"GasSales"] = as.numeric(enertia_production[,"GasSales"])

          names(enertia_production) = c("WellCompCode", "Date", "OIL", "GAS", "PPROD", "WATER", "GAS_INJ", "WATER_INJ", "Oil_Sales", "Gas_Sales")
          # Close ODBC data connection to free up datasource.
          RODBC::odbcClose(channel = db_handle)

          if(!is.null(enertia_production)){
              # Replace all NA values within product columns with a 0
              enertia_production[is.na(enertia_production)] = 0
              # Convert PPROD units from gallons to stb (42gal = 1stb)
              enertia_production$PPROD = enertia_production$PPROD/42
          }

          # Export the data to excel file if user sets export = TRUE
          if(export == TRUE){
              openxlsx::write.xlsx(x = enertia_production[, c("WellCompCode", "Date", "OIL", "GAS", "PPROD", "WATER", "GAS_INJ", "WATER_INJ", "Oil_Sales", "Gas_Sales")],
                                   file = paste0(Sys.getenv("USERPROFILE"),
                                                 "\\DESKTOP\\", "Daily_Prod ",
                                                 strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                                 ".xlsx")
                                   )
          }
          
        return(enertia_production[, c("WellCompCode", "Date", "OIL", "GAS", "PPROD", "WATER", "GAS_INJ", "WATER_INJ", "Oil_Sales", "Gas_Sales")])

      },
      error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
      }

    )


} # Complete


#' Retrieve Enertia Lineage as a data frame.
#'
#' @param asset_team_code A string.
#' @param export A bolean.
#' @return A dataframe containing the Enertia Lineage for a set of Asset Team Codes
#' @examples
#' get_Enertia_Lineage_by_Asset_Team("RMR")
#' get_Enertia_Lineage_by_Asset_Team(c("RMR", "WLA", "ELA")
#' @description
#' The following functin will make a connection to the ofm database located on the Enertia01B server.
#' It will then take the asset_team_code vector which contains a sequence of Enertia asset team codes to query.
#' The function will then return a dataframe object with all enertia entities that exists for each
#' asset team code within the asset_team_code vector.
get_Enertia_Lineage_by_Asset_Team = function(asset_team_code, export = FALSE){

    tryCatch({
        db_handle_enertia01b = get_dbhandle(server = "ENERTIA01B", database = "HEC_Repository")

        #Compile Query
        query_enertia01b_lineage = paste0("SELECT ofm_Lineage.AssetTeamCode,
                                          ofm_Master.AssetTeam,
                                          ofm_Lineage.WellboreCode,
                                          ofm_Lineage.WellboreName,
                                          ofm_Lineage.WellCompCode,
                                          ofm_Lineage.WellCompName,
                                          ofm_Master.ApiNumber12,
                                          ofm_Master.Reservoir,
                                          ofm_Lineage.FieldCode,
                                          ofm_Lineage.FieldName,
                                          ofm_Lineage.SubFieldCode,
                                          ofm_Lineage.SubFieldName,
                                          ofm_Lineage.FacPlatCode,
                                          ofm_Lineage.FacPlatName,
                                          ofm_Master.State,
                                          ofm_Master.County,
                                          ofm_Master.OperatorName
                                  FROM ofm_Master INNER JOIN ofm_Lineage ON ofm_Master.WellCompHID = ofm_Lineage.WellCompHID
                                  WHERE ofm_Lineage.AssetTeamCode IN (", paste0("'", asset_team_code, collapse ="',") ,"');")
        enertia_lineage = data.frame(RODBC::sqlQuery(db_handle_enertia01b, query_enertia01b_lineage))
        enertia_lineage[] = lapply(enertia_lineage, trimws)
        enertia_lineage[] = lapply(enertia_lineage, as.character)

        # Close ODBC data connection to free up datasource.
        RODBC::odbcClose(channel = db_handle_enertia01b)

        enertia_lineage = enertia_lineage[, c(1,2,3,4,5,6,7,8,10,9,12,11,13,14,17,15,16)]
        names(enertia_lineage) = c("AssetTeamCode",
                                   "AssetTeam",
                                   "WellBoreCode",
                                   "WellBoreName",
                                   "WellCompCode",
                                   "WellCompName",
                                   "APINumber",
                                   "Reservoir",
                                   "FieldName",
                                   "FieldCode",
                                   "SubFieldName",
                                   "SubFieldCode",
                                   "FacPlatCode",
                                   "FacPlatName",
                                   "OperatorName",
                                   "State",
                                   "County")
        
        # Export the data to excel file if user sets export = TRUE
        if(export == TRUE){
            openxlsx::write.xlsx(x = enertia_lineage[!duplicated(enertia_lineage$WellCompCode), ],
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "Lineage ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx")
                                 )
        }

        return(enertia_lineage[!duplicated(enertia_lineage$WellCompCode), ])


    },error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })


} # Complete

#' Retrieve Enertia Lineage as a data frame.
#'
#' @param field_code A string.
#' @param export A bolean.
#' @return A dataframe containing the Enertia Lineage of a set of Asset Team Codes
#' @examples
#' get_Enertia_Lineage_by_Field_Code("49.1017")
#' get_Enertia_Lineage_by_Field_Code(c("49.1017", "49.1015")
#' @description
#' The following functin will make a connection to the ofm database located on the Enertia01B server.
#' It will then take the field_code vector which contains a sequence of Enertia field codes to query.
#' The function will then return a dataframe object with all enertia entities that exists for each
#' field code within the field_code vector.
get_Enertia_Lineage_by_Field_Code = function(field_code, export = FALSE){

    tryCatch({
        db_handle_enertia01b = get_dbhandle(server = "ENERTIA01B", database = "HEC_Repository")

        #Compile Query
        query_enertia01b_lineage = paste0("SELECT ofm_Lineage.AssetTeamCode,
                                          ofm_Master.AssetTeam,
                                          ofm_Lineage.WellboreCode,
                                          ofm_Lineage.WellboreName,
                                          ofm_Lineage.WellCompCode,
                                          ofm_Lineage.WellCompName,
                                          ofm_Master.ApiNumber12,
                                          ofm_Master.Reservoir,
                                          ofm_Lineage.FieldCode,
                                          ofm_Lineage.FieldName,
                                          ofm_Lineage.SubFieldCode,
                                          ofm_Lineage.SubFieldName,
                                          ofm_Lineage.FacPlatCode,
                                          ofm_Lineage.FacPlatName,
                                          ofm_Master.State,
                                          ofm_Master.County,
                                          ofm_Master.OperatorName
                                  FROM ofm_Master INNER JOIN ofm_Lineage ON ofm_Master.WellCompHID = ofm_Lineage.WellCompHID
                                  WHERE ofm_Lineage.FieldCode IN (", paste0("'", field_code, collapse ="',") ,"');")
        enertia_lineage = data.frame(RODBC::sqlQuery(db_handle_enertia01b, query_enertia01b_lineage))
        enertia_lineage[] = lapply(enertia_lineage, trimws)
        enertia_lineage[] = lapply(enertia_lineage, as.character)

        # Close ODBC data connection to free up datasource.
        RODBC::odbcClose(channel = db_handle_enertia01b)
        enertia_lineage = enertia_lineage[, c(1,2,3,4,5,6,7,8,10,9,12,11,13,14,17,15,16)]
        names(enertia_lineage) = c("AssetTeamCode",
                                   "AssetTeam",
                                   "WellBoreCode",
                                   "WellBoreName",
                                   "WellCompCode",
                                   "WellCompName",
                                   "APINumber",
                                   "Reservoir",
                                   "FieldName",
                                   "FieldCode",
                                   "SubFieldName",
                                   "SubFieldCode",
                                   "FacPlatCode",
                                   "FacPlatName",
                                   "OperatorName",
                                   "State",
                                   "County")
        
        # Export the data to excel file if user sets export = TRUE
        if(export == TRUE){
            openxlsx::write.xlsx(x = enertia_lineage[!duplicated(enertia_lineage$WellCompCode), ],
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "Lineage ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx")
                                 )
        }
        
        return(enertia_lineage[!duplicated(enertia_lineage$WellCompCode), ])


    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })

} #Complete

#' Retrieve Enertia Lineage as a data frame.
#'
#' @param enertia_code A string.
#' @param export A bolean.
#' @return A dataframe containing the Enertia Lineage of a set of Asset Team Codes
#' @examples
#' get_Enertia_Lineage_by_Enertia_Code("49.1017.0007.00")
#' get_Enertia_Lineage_by_Enertia_Code(c("49.1017.0007.00", "49.1017.0023.00"))
#' @description
#' The following functin will make a connection to the ofm database located on the Enertia01B server.
#' It will then take the enertia_code vector which contains a sequence of enertia codes to query.
#' The function will then return a dataframe object with all enertia entities that exists for each
#' enertia code within the enertia_code vector.
get_Enertia_Lineage_by_Enertia_Code = function(enertia_code, export = FALSE){

    tryCatch({
        db_handle_enertia01b = get_dbhandle(server = "ENERTIA01B", database = "HEC_Repository")

        #Compile Query
        query_enertia01b_lineage = paste0("SELECT ofm_Lineage.AssetTeamCode,
                                          ofm_Master.AssetTeam,
                                          ofm_Lineage.WellboreCode,
                                          ofm_Lineage.WellboreName,
                                          ofm_Lineage.WellCompCode,
                                          ofm_Lineage.WellCompName,
                                          ofm_Master.ApiNumber12,
                                          ofm_Master.Reservoir,
                                          ofm_Lineage.FieldCode,
                                          ofm_Lineage.FieldName,
                                          ofm_Lineage.SubFieldCode,
                                          ofm_Lineage.SubFieldName,
                                          ofm_Lineage.FacPlatCode,
                                          ofm_Lineage.FacPlatName,
                                          ofm_Master.State,
                                          ofm_Master.County,
                                          ofm_Master.OperatorName
                                  FROM ofm_Master INNER JOIN ofm_Lineage ON ofm_Master.WellCompHID = ofm_Lineage.WellCompHID
                                  WHERE ofm_Lineage.WellCompCode IN (", paste0("'", enertia_code, collapse ="',") ,"');")
        enertia_lineage = data.frame(RODBC::sqlQuery(db_handle_enertia01b, query_enertia01b_lineage))
        enertia_lineage[] = lapply(enertia_lineage, trimws)
        enertia_lineage[] = lapply(enertia_lineage, as.character)
        # Close ODBC data connection to free up datasource.
        RODBC::odbcClose(channel = db_handle_enertia01b)

        enertia_lineage = enertia_lineage[, c(1,2,3,4,5,6,7,8,10,9,12,11,13,14,17,15,16)]
        names(enertia_lineage) = c("AssetTeamCode",
                                   "AssetTeam",
                                   "WellBoreCode",
                                   "WellBoreName",
                                   "WellCompCode",
                                   "WellCompName",
                                   "APINumber",
                                   "Reservoir",
                                   "FieldName",
                                   "FieldCode",
                                   "SubFieldName",
                                   "SubFieldCode",
                                   "FacPlatCode",
                                   "FacPlatName",
                                   "OperatorName",
                                   "State",
                                   "County")
        
        # Export the data to excel file if user sets export = TRUE
        if(export == TRUE){
            openxlsx::write.xlsx(x = enertia_lineage[!duplicated(enertia_lineage$WellCompCode), ],
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "Lineage ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx")
                                 )
        }

        return(enertia_lineage[!duplicated(enertia_lineage$WellCompCode), ])

    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })


} # Complete


#' Retrieve Enertia Lineage as a data frame.
#'
#' @param API12 A string.
#' @param export A bolean.
#' @return A dataframe containing the Enertia Lineage for a specified vector of API12 Numbers
#' @examples
#' get_Enertia_Lineage_by_API("5002921693")
#' get_Enertia_Lineage_by_API(c("5002921693", "5002921905"))
#' @description
#' The following functin will make a connection to the ofm database located on the Enertia01B Server.
#' It will then take the API12 vector which contains a sequence of API12 Numbers to query.
#' The function will then return a dataframe object with all enertia entities that exists for each
#' API12 within the API12 vector.
get_Enertia_Lineage_by_API12 = function(API12, export = FALSE){

    tryCatch({
        db_handle_enertia01b = get_dbhandle(server = "ENERTIA01B", database = "HEC_Repository")

        if(grepl(pattern = "-", x = API12)){API12 = stringr::str_replace_all(string = API12, pattern = "-", replacement = "")}
        if(nchar(API12) > 12){API12 = substr(API12, start = 1, stop = 12)}

        #Compile Query
        query_enertia01b_lineage = paste0("SELECT ofm_Lineage.AssetTeamCode,
                                          ofm_Master.AssetTeam,
                                          ofm_Lineage.WellboreCode,
                                          ofm_Lineage.WellboreName,
                                          ofm_Lineage.WellCompCode,
                                          ofm_Lineage.WellCompName,
                                          ofm_Master.ApiNumber12,
                                          ofm_Master.Reservoir,
                                          ofm_Lineage.FieldCode,
                                          ofm_Lineage.FieldName,
                                          ofm_Lineage.SubFieldCode,
                                          ofm_Lineage.SubFieldName,
                                          ofm_Lineage.FacPlatCode,
                                          ofm_Lineage.FacPlatName,
                                          ofm_Master.State,
                                          ofm_Master.County,
                                          ofm_Master.OperatorName
                                  FROM ofm_Master INNER JOIN ofm_Lineage ON ofm_Master.WellCompHID = ofm_Lineage.WellCompHID
                                  WHERE ofm_Master.ApiNumber12 IN (", paste0("'", API12, collapse ="',") ,"');")
        enertia_lineage = data.frame(RODBC::sqlQuery(db_handle_enertia01b, query_enertia01b_lineage))
        enertia_lineage[] = lapply(enertia_lineage, trimws)
        enertia_lineage[] = lapply(enertia_lineage, as.character)

        # Close ODBC data connection to free up datasource.
        RODBC::odbcClose(channel = db_handle_enertia01b)

        enertia_lineage = enertia_lineage[, c(1,2,3,4,5,6,7,8,10,9,12,11,13,14,17,15,16)]
        names(enertia_lineage) = c("AssetTeamCode",
                                   "AssetTeam",
                                   "WellBoreCode",
                                   "WellBoreName",
                                   "WellCompCode",
                                   "WellCompName",
                                   "APINumber",
                                   "Reservoir",
                                   "FieldName",
                                   "FieldCode",
                                   "SubFieldName",
                                   "SubFieldCode",
                                   "FacPlatCode",
                                   "FacPlatName",
                                   "OperatorName",
                                   "State",
                                   "County")
        
        # Export the data to excel file if user sets export = TRUE
        if(export == TRUE){
            openxlsx::write.xlsx(x = enertia_lineage[!duplicated(enertia_lineage$WellCompCode), ],
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "Lineage ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx")
                                 )
        }
        
        return(enertia_lineage[!duplicated(enertia_lineage$WellCompCode), ])
    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })


} # Complete


#' Retrieve Aries-Enertia links as a data frame for a user defined vector of asset team codes.
#'
#' @param asset_team_code a character vector.
#' @param export a boolean value
#' @return A dataframe containing the Aries-Enertia links for a specified vector of asset team codes
#' @examples
#' get_Aries_Enertia_Links_by_Area("RMR", export = TRUE)
#' @description
#' The following functin will make a connection to the Enertia database located on the Enertia01B Server.
#' It will then take the user defined 'asset_team_code' vector which contains one or more asset teams to query.
#' The function will then return a dataframe object with all the current aries_enertia links that exists at the time of the query.
#' the export variable can be set equal to TRUE to export the output to the users desktop.
get_Aries_Enertia_Links_by_Area = function(asset_team_code, export = FALSE){

    tryCatch({
        # Create database connection handles to both the Enertia and HEC_Enertia databases located on the Enertia01B server.
        db_handle_hec_enertia = get_dbhandle(server = "enertia01b", database = "HEC_Enertia")
        db_handle_enertia = get_dbhandle(server = "enertia01b", database = "Enertia")

        # Define a query for the lineage that is based on the specified list of asset team codes.
        lineage_query = paste0("SELECT WellCompHID,
                                        WellCompCode,
                                        WellCompName,
                                        FieldName,
                                        FieldCode,
                                        SubFieldName,
                                        SubFieldCode,
                                        FacPlatName,
                                        FacPlatCode,
                                        APINumber,
                                        OperatorName,
                                        State,
                                        County
                                FROM pdRptWellCompletionAssetTeamHierarchy
                                WHERE AssetTeamCode IN ('", paste(asset_team_code, collapse = "', '"), "');")
        lineage = sqlQuery(channel = db_handle_enertia, query = lineage_query)

        # Define a query based on the WellCompHID that exist in the lineage table queried above where 'IsDeleted' is 0 meaning it's an active link.
        aries_enertia_query = paste0("SELECT *
                                      FROM ac_EnertiaMapping
                                      WHERE (EnertiaHID IN('", paste(as.character(lineage$WellCompHID), collapse = "', '"), "')  AND
                                           (IsDeleted = '0'));")
        Aries_Enertia_Links = sqlQuery(channel = db_handle_hec_enertia, query = aries_enertia_query)

        # Inner join the lineage table to the Aries_Enertia_Links to combine contents of both tables. Also trim any unnessesary whitespace in the tables
        # and remove duplicates from the join due to the lineage query
        Aries_Enertia_Links = inner_join(Aries_Enertia_Links, lineage, by = c("EnertiaHID" = "WellCompHID")) %>%
                              lapply(., trimws) %>%
                              data.frame(stringsAsFactors = FALSE)
        Aries_Enertia_Links = Aries_Enertia_Links[!duplicated(Aries_Enertia_Links$WellCompCode), ]
        # Close all odbc connections and return the Aries_Enertia_Links table with columns rearranged.
        odbcCloseAll()

        # Export to excel on user desktop if export is set equal to true.
        if(export == TRUE){
            openxlsx::write.xlsx(Aries_Enertia_Links[, c("Seqnum", "WellCompCode", "WellCompName", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "FacPlatName", "FacPlatCode", "APINumber", "OperatorName", "State", "County", "CreateUser", "CreateDate", "ChangeUser", "ChangeDate")],
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "Aries-Enertia Links ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx"))
        }
        return(Aries_Enertia_Links[, c("Seqnum", "WellCompCode", "WellCompName", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "FacPlatName", "FacPlatCode", "APINumber", "OperatorName", "State", "County", "CreateUser", "CreateDate", "ChangeUser", "ChangeDate")])


    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })

} # Completed

#' Retrieve Aries-Enertia links as a data frame for a user defined vector of enertia codes.
#'
#' @param enertia_codes a character vector.
#' @param export a boolean value
#' @return A dataframe containing the Aries-Enertia links for a specified vector of enertia codes
#' @examples
#' get_Aries_Enertia_Links_by_Enertia_Code("49.1018.0010.00", export = TRUE)
#' @description
#' The following functin will make a connection to the Enertia database located on the Enertia01B Server.
#' It will then take the user defined 'enertia_codes' vector which contains one or more enertia codes to query.
#' The function will then return a dataframe object with all the current aries_enertia links that exists at the time of the query.
#' the export variable can be set equal to TRUE to export the output to the users desktop.
get_Aries_Enertia_Links_by_Enertia_Code = function(enertia_codes, export = FALSE){

    tryCatch({
        # Create database connection handles to both the Enertia and HEC_Enertia databases located on the Enertia01B server.
        db_handle_hec_enertia = get_dbhandle(server = "enertia01b", database = "HEC_Enertia")
        db_handle_enertia = get_dbhandle(server = "enertia01b", database = "Enertia")

        # Define a query for the lineage that is based on the specified list of asset team codes.
        lineage_query = paste0("SELECT WellCompHID,
                                        WellCompCode,
                                        WellCompName,
                                        FieldName,
                                        FieldCode,
                                        SubFieldName,
                                        SubFieldCode,
                                        FacPlatName,
                                        FacPlatCode,
                                        APINumber,
                                        OperatorName,
                                        State,
                                        County
                                FROM pdRptWellCompletionAssetTeamHierarchy
                                WHERE WellCompCode IN ('", paste(enertia_codes, collapse = "', '"), "');")
        lineage = sqlQuery(channel = db_handle_enertia, query = lineage_query)

        # Define a query based on the WellCompHID that exist in the lineage table queried above where 'IsDeleted' is 0 meaning it's an active link.
        aries_enertia_query = paste0("SELECT *
                                      FROM ac_EnertiaMapping
                                      WHERE (EnertiaHID IN('", paste(as.character(lineage$WellCompHID), collapse = "', '"), "')  AND
                                           (IsDeleted = '0'));")
        Aries_Enertia_Links = sqlQuery(channel = db_handle_hec_enertia, query = aries_enertia_query)

        # Inner join the lineage table to the Aries_Enertia_Links to combine contents of both tables. Also trim any unnessesary whitespace in the tables
        # and remove duplicates from the join due to the lineage query
        Aries_Enertia_Links = inner_join(Aries_Enertia_Links, lineage, by = c("EnertiaHID" = "WellCompHID")) %>%
                              lapply(., trimws) %>%
                              data.frame(stringsAsFactors = FALSE)
        Aries_Enertia_Links = Aries_Enertia_Links[!duplicated(Aries_Enertia_Links$WellCompCode), ]
        # Close all odbc connections and return the Aries_Enertia_Links table with columns rearranged.
        odbcCloseAll()


        # Export to excel on user desktop if export is set equal to true.
        if(export == TRUE){
            openxlsx::write.xlsx(Aries_Enertia_Links[, c("Seqnum", "WellCompCode", "WellCompName", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "FacPlatName", "FacPlatCode", "APINumber", "OperatorName", "State", "County", "CreateUser", "CreateDate", "ChangeUser", "ChangeDate")],
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "Aries-Enertia Links ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx"))
        }
        return(Aries_Enertia_Links[, c("Seqnum", "WellCompCode", "WellCompName", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "FacPlatName", "FacPlatCode", "APINumber", "OperatorName", "State", "County", "CreateUser", "CreateDate", "ChangeUser", "ChangeDate")])

    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })

} # Completed

#' QC the existing Aries-Enertia links and lineage by returning a data frame for a user defined vector of asset team code.
#'
#' @param area a character vector.
#' @param export a boolean value
#' @return A dataframe containing QC flags for Aries-Enertia links and associated lineage
#' @examples
#' QC_Aries_Enertia_Links(c("ANS", "CIO"), export = TRUE)
#' @description
#' The following functin will make a connection to the Enertia database located on the Enertia01B Server.
#' It will then take the user defined 'area' vector which contains one or more asset team codes to query.
#' The function will then return a dataframe object which contains QC flags related to the existing aries-enertia links and the associated lineage.
#' #' the export variable can be set equal to TRUE to export the output to the users desktop.
QC_Aries_Enertia_Links = function(area, export = FALSE){

    tryCatch({
        # Get Aries-Enertia Links using Reserves.Util Package. Define a table for flags
        links = get_Aries_Enertia_Links_by_Area(area %>% as.character())
        flags = data.frame(Seqnum = unique(links$Seqnum),
                           Flag_Type = rep("", length(unique(links$Seqnum))),
                           stringsAsFactors = FALSE)

        # Loop through each unique seqnum in the links table and run individual QC checks
        for(i in flags$Seqnum){

            # Check for multiple API10 asigned to single Aries Case.
            API_check = data.frame(API = links[links$Seqnum == i, "APINumber"])
            API_check$API = substr(API_check$API, 1, 10)
            if(length(unique(API_check$API)) > 1){
              flags[flags$Seqnum == i, "Flag_Type"] = paste0(flags[flags$Seqnum == i, "Flag_Type"], "MULTIPLE API10", sep = " // ")
            }

            # ---- // INSERT ADDITIONAL CHECKS // ----
        }

        # Export to excel on user desktop if export is set equal to true.
        if(export == TRUE){
            openxlsx::write.xlsx(flags,
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "Aries-Enertia Link QC ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx"))
        }

        # Output table of unique seqnum and any flags that have been found
        return(flags)

    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })


}

#' Create an AC_PRODUCT table consisting of aries cases linked to the list of speicifed enertia codes.
#'
#' @param enertia_codes a character vector.
#' @param server a character vector.
#' @param database a character vector.
#' @param user a character vector.
#' @param overwrite a character vector.
#' @param export a boolean value
#' @return A dataframe containing production data formated for the AC_PRODUCT table in Aries
#' @examples
#' create_AC_Product_by_Link(enertia_codes = c("49.1018.0010.00", "49.1018.0210.00"), user = "user01", overwrite = "N", export = TRUE)
#' @description
#' The following functin will make a connection to the Enertia database located on the Enertia01B Server.
#' It will then take the user defined 'enertia_codes' vector which contains one or more enertia_codes to query.
#' The function will then return a dataframe object which contains aries cases linked to the specified enertia codes and the associated production data.
#' the data frame is returned in the same format as the Aries AC_PRODUCT table.
#' the export variable can be set equal to TRUE to export the output to the users desktop.
create_AC_Product_by_Link = function(enertia_codes, server = "extsql01", database = "Aries", user = Sys.getenv("USERNAME"), overwrite = "Y", export = FALSE){

    tryCatch({
        # Query the enertia and repository production for the user speicified list of enertia codes.
        prod = get_Enertia_Production(well_comp_codes = as.character(enertia_codes))
        prod$Date = paste0(prod$Date %>% lubridate::month(), "/", prod$Date %>% lubridate::day(), "/", prod$Date %>% lubridate::year())

        # Query the Aries-Enertia links for the user specified list of enertia codes.
        links = get_Aries_Enertia_Links_by_Enertia_Code(as.character(enertia_codes))

        # Query the Ac_property table for the seqnums that exist in the links table. We will be using this table to tie Aries to the
        # Enertia/Repository production based on existing Aries-Enertia links
        ac_property = sqlQuery(channel = get_dbhandle(server = server, database = database),
                               query = paste0("SELECT SEQNUM,
                                                      PROPNUM
                                              FROM Ariesadmin.AC_PROPERTY
                                              WHERE SEQNUM IN ('", paste(as.character(links$Seqnum), collapse = "', '"), "')"))  %>%
                              data.frame(stringsAsFactors = FALSE)
        # Close all open odbc connections
        odbcCloseAll()

        # Inner Join the enertia/repository production table to the links table and subsequently to the ac_property table.
        # additional columns are added to match the structure of the Aries AC_Product table.
        ac_property$SEQNUM = as.character(ac_property$SEQNUM)
        ac_product = dplyr::inner_join(prod, links[, c("Seqnum", "WellCompCode")], by = "WellCompCode") %>%
                     dplyr::inner_join(., ac_property[, c("SEQNUM", "PROPNUM")], by = c("Seqnum" = "SEQNUM")) %>%
                     data.frame(.,
                                FTP = rep(NA, nrow(.)),
                                WELL = rep(NA, nrow(.)),
                                Overwrite = rep(overwrite, nrow(.)),
                                ChangeDate = rep(strftime(Sys.Date(), "%m/%d/%Y"), nrow(.)),
                                ChangeUser = rep(user, nrow(.)), stringsAsFactors = FALSE)
        ac_product = ac_product[ , c("PROPNUM", "Date", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ", "FTP", "WELL", "Overwrite", "ChangeDate", "ChangeUser")]
        names(ac_product) =  c("PROPNUM", "P_DATE", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ", "FTP", "WELL", "Overwrite", "ChangeDate", "ChangeUser")
        
        # Roll up multizone wells this should create a table with a unique combination of PROPNUM + Date
        ac_product = ac_product %>%
                        dplyr::group_by(PROPNUM,
                                        P_DATE,
                                        FTP,
                                        WELL,
                                        Overwrite,
                                        ChangeDate,
                                        ChangeUser) %>%
                            dplyr::summarise(OIL = sum(OIL),
                                             GAS = sum(GAS),
                                             WATER = sum(WATER),
                                             GAS_INJ = sum(GAS_INJ),
                                             WATER_INJ = sum(WATER_INJ))

        # If the user sets export to TRUE, the output is saved to thier desktop
        if(export == TRUE){
            openxlsx::write.xlsx(x = split(ac_product[ , c("PROPNUM", "P_DATE", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ", "FTP", "WELL", "Overwrite", "ChangeDate", "ChangeUser")],
                                           rep(1:ceiling(nrow(ac_product)/1000000), each=1000000, length.out = nrow(ac_product))),
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "AC_PRODUCT ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx")
                                 )
        }

        # Return the product table in the same structure as the Ac_product table in Aries
        return(ac_product[ , c("PROPNUM", "P_DATE", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ", "FTP", "WELL", "Overwrite", "ChangeDate", "ChangeUser")])

    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })

} # Completed


#' Create an AC_PRODUCT table consisting of enertia codes linked to the list of speicifed Aries propnum.
#'
#' @param propnum a character vector.
#' @param server a character vector.
#' @param database a character vector.
#' @param user a character vector.
#' @param overwrite a character vector.
#' @param export a boolean value
#' @return A dataframe containing production data formated for the AC_PRODUCT table in Aries
#' @examples
#' create_AC_Product_by_Enertia_Code(enertia_codes = c("0WHEZJ4SQ4ZJ", "YXK78QADR9BZ"), user = "user01", overwrite = "N", export = TRUE)
#' @description
#' The following functin will make a connection to an Aries database.
#' It will then take the user defined 'propnum' vector which contains one or more Aries Propnum to query.
#' The function will then return a dataframe object which contains enertia codes linked to the specified ariespropnum and the associated production data.
#' the data frame is returned in the same format as the Aries AC_PRODUCT table.
#' the export variable can be set equal to TRUE to export the output to the users desktop.
create_AC_Product_by_Enertia_Code = function(propnum, server = "extsql01", database = "Aries", user = Sys.getenv("USERNAME"), overwrite = "Y", export = FALSE){

    tryCatch({
        # Connect to the aries database and retrieve the enertia codes for the list of propnum
        db = get_dbhandle(server = server, database = database)
        master = sqlQuery(channel = db, query = paste0("SELECT Ariesadmin.AC_PROPERTY.PROPNUM,
                                                                      Ariesadmin.AC_PROPERTY.ENERTIA_CODE
                                                               FROM Ariesadmin.AC_PROPERTY
                                                               WHERE Ariesadmin.AC_PROPERTY.PROPNUM in ('", paste0(as.character(propnum), collapse = "','"), "');"))
        odbcCloseAll()

        master$ENERTIA_CODE = gsub("\\s+", " ", master$ENERTIA_CODE)
        master = lapply(X = str_split(master$ENERTIA_CODE %>% as.character(), ","),
                                       function(x){

                                           length(x)=7
                                           return(data.frame(t(x), stringsAsFactors = FALSE))

                                       }) %>% bind_rows() %>% cbind(PROPNUM = master$PROPNUM, .)

        master = tidyr::gather(master, value = "WellCompCode", key = "key", c(names(master)[-1]))[, c("PROPNUM", "WellCompCode")]
        master = master[complete.cases(master), ]
                names(master) = c("PROPNUM", "WellCompCode")
        master$WellCompCode = trimws(master$WellCompCode)

        # Query all data for each enertia code related to each unique propnum
        ac_product = (get_Enertia_Production(well_comp_codes = master$WellCompCode) %>%
                        inner_join(., y = master, by = "WellCompCode"))[, c("PROPNUM", "Date", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ")]
        ac_product = data.frame(ac_product, FTP = "", WELL = "", Overwrite = overwrite, ChangeDate = Sys.Date(), ChangeUser = user, stringsAsFactors = FALSE)
        names(ac_product) = c("PROPNUM", "P_DATE", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ", "FTP", "WELL", "Overwrite", "ChangeDate", "ChangeUser")

        # # Roll up all numbers by propnum
        ac_product = ac_product %>%
                        dplyr::group_by(PROPNUM,
                                        P_DATE,
                                        FTP,
                                        WELL,
                                        Overwrite,
                                        ChangeDate,
                                        ChangeUser) %>%
                            dplyr::summarise(OIL = sum(OIL),
                                             GAS = sum(GAS),
                                             WATER = sum(WATER),
                                             GAS_INJ = sum(GAS_INJ),
                                             WATER_INJ = sum(WATER_INJ))

        # Export the data to excel file if user sets export = TRUE
        if(export == TRUE){
            openxlsx::write.xlsx(x = split(ac_product[ , c("PROPNUM", "P_DATE", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ", "FTP", "WELL", "Overwrite", "ChangeDate", "ChangeUser")],
                                           rep(1:ceiling(nrow(ac_product)/1000000), each=1000000, length.out = nrow(ac_product))),
                                 file = paste0(Sys.getenv("USERPROFILE"),
                                               "\\DESKTOP\\", "AC_PRODUCT ",
                                               strftime(Sys.time(), "%m%d%y%-%I%M%p"),
                                               ".xlsx")
                                 )
        }


        return(ac_product[, c("PROPNUM", "P_DATE", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ", "FTP", "WELL", "Overwrite", "ChangeDate", "ChangeUser")])

    }, error = function(cond){

        return(data.frame(ERROR = c("INVALID INPUTS OR UNABLE TO MAKE CONNECTION WITH SERVER")))
    })

} # Complete






