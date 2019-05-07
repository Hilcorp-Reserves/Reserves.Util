# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'
library(RODBC)
library(magrittr)
library(dplyr)
library(reshape2)


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

#' Retrieve Aries economic data.
#'
#' @param server A string.
#' @param database A string.
#' @param PROPNUM A String
#' @param scenario_name A string
#' @param view_by A string
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
get_Economics = function(server, database, PROPNUM, scenario_name, view_by = "Scenario"){

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

    economics_by_section = list(sect_2_econ, sect_4_econ, sect_5_econ, sect_6_econ, sect_7_econ, sect_8_econ, sect_9_econ)
  return(economics_by_section)

}  #Complete

#' Retrieve Enertia Production Data.
#'
#' @param WellCompCodes A string.
#' @return A dataframe containing Production data
#' @examples
#' get_Enertia_Production("49.1016.0022.00")
#' @description
#' The following functin will make a defauly connection to the Enertia_Reports database located on the Default Enertia04 Server.
#' It will then take the WellCompCodes vector which contains a sequence of Enertia Completion Codes to query.
#' The function will then return a dataframe object with all the enertia production data that exists for each
#' Enertia Completion Code within the WellCompCodes vector.
get_Enertia_Production = function(WellCompCodes){
    #Connect to Datasource
    db_handle = get_dbhandle(server = "ENERTIA04", database = "Enertia_Reports")
    #Compile Query
    query = paste0("SELECT pdRptWellCompletionAssetTeamHierarchy.WellCompCode,
                           pdRptWellCompletionAssetTeamHierarchy.WellCompName,
                           pdMasProdAllocDetail.DtlProdDate,
                           pdMasProdAllocDetail.DtlProdCode,
                           pdMasProdAllocDetail.DtlProdDisp,
                           pdMasProdAllocDetail.DtlVolume,
                           pdRptWellCompletionAssetTeamHierarchy.FieldName,
                           pdRptWellCompletionAssetTeamHierarchy.SubFieldName,
                           pdRptWellCompletionAssetTeamHierarchy.AssetTeamCode
                           FROM pdMasProdAllocDetail
                           INNER JOIN pdRptWellCompletionAssetTeamHierarchy
                           ON pdMasProdAllocDetail.DtlPropHID = pdRptWellCompletionAssetTeamHierarchy.WellCompHID
                             WHERE pdRptWellCompletionAssetTeamHierarchy.WellCompCode In (", paste0("'", WellCompCodes, collapse ="',") ,"')
                               AND pdMasProdAllocDetail.DtlProdDisp = 'FRM';")
    # Query the database, trim any excess white space, convert all factor columns to character, and convert the volumes to numeric.
    enertia_production = data.frame(RODBC::sqlQuery(db_handle, query))
    enertia_production[] = lapply(enertia_production, trimws)
    enertia_production[] = lapply(enertia_production, as.character)
    enertia_production[,"DtlVolume"] = as.numeric(enertia_production[,"DtlVolume"])

    # PIVOT table by production code
    enertia_production = reshape2::dcast(data = enertia_production,
                                         formula = WellCompCode+DtlProdDate~DtlProdCode,
                                         value.var = "DtlVolume",
                                         fun.aggregate = sum)

    # Close ODBC data connection to free up datasource.
    RODBC::odbcClose(channel = db_handle)
    # Replace all NA values within product columns with a 0
    enertia_production[is.na(enertia_production)] = 0


  return(enertia_production)
}
