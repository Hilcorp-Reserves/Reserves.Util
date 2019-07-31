# Reserves_Util Package v0.1.0
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

#' Convert Aries Lookup Tables to Hard Coded Lines
#'
#' @param economics_by_section Dataframe object containing economic lines for each Aries econonmic section.
#' @param ar_lookup Dataframe containing the LOOKUP tables from an Aries database.
#' @param ac_property Dataframe containing the Master table from and Aries database.
#' @return Returns a dataframe object containing hard coded economic lines from Aries.
#' @examples
#' Lookup_to_Hard_Coded(economics_by_section, ar_lookup, ac_property)
#' @description
#' This function will convert Aries LOOKUP tables within the Economics_by Section dataframe object into hard coded lines.
Lookup_to_Hard_Coded = function(economics_by_section, ar_lookup, ac_property){

  FINAL_Inputs = dplyr::bind_rows(lapply(X = unique(economics_by_section[economics_by_section$KEYWORD == "LOOKUP", "EXPRESSION"]),
                                  FUN = function(i){
      lookup_tab = ar_lookup[ar_lookup$NAME == strsplit(i, " ")[[1]][1], c(1:12)]
      lookup_items = data.frame(lookup_tab[lookup_tab$LINETYPE == 0, "VAR0"], stringsAsFactors = FALSE)
      lookup_parms = lookup_tab[lookup_tab$LINETYPE == 0, seq(5,11,1)]
      if(is.element(FALSE, !is.na(t(lookup_parms)))){
          lookup_parms = t(t(lookup_parms)[!is.na(t(lookup_parms)),])
      }

      cat_index_M = grep(lookup_tab[lookup_tab$LINETYPE == 1 & lookup_tab$SEQUENCE == 1, c(4:ncol(lookup_tab))], pattern = "M") + 3
      index = t(which(t(lookup_parms) == "?", arr.ind = TRUE))
      cat_index_NCL = seq(max(cat_index_M)+1, by = 1, length.out = ncol(index))

      if(grepl(pattern = '"', x = lookup_items)){
          for(j in seq(1,nrow(lookup_items))){
              if(lookup_items[j,1] == "\""){lookup_items[j,1] = lookup_items[j-1,1]}
          }
      }

      lookup_tab = lookup_tab[lookup_tab$LINETYPE ==3, c(1:max(cat_index_NCL))]

      if(length(cat_index_M)>1){
          field_match = data.frame(field_match = apply(lookup_tab[ , as.numeric(cat_index_M)] , 1 , paste , collapse = "" ),
                                  stringsAsFactors = FALSE)
          lookup_match = data.frame(strsplit(strsplit(i, "@M.")[[1]], " "),
                                  stringsAsFactors = FALSE)
      }
      if(length(cat_index_M)==1){
          field_match = data.frame(field_match = lookup_tab[, "VAR0"],
                                  stringsAsFactors = FALSE)
          lookup_match = data.frame(strsplit(strsplit(i, "@M.")[[1]], " "),
                                  stringsAsFactors = FALSE)
      }

      lookup_tab = cbind(NAME = lookup_tab[,"NAME"],
                        field_match = field_match,
                        lookup_tab[, as.numeric(cat_index_NCL)])

      FINAL = dplyr::bind_rows(lapply(X = unique(economics_by_section[(economics_by_section$EXPRESSION == i & economics_by_section$KEYWORD == "LOOKUP"), "PROPNUM"]),
                               FUN = function(x){

          lookup_parms_add = lookup_parms
          if(length(cat_index_M)>1){
              field_code = data.frame(field_code = apply(ac_property[ac_property$PROPNUM == x, as.character(lookup_match[2:length(lookup_match)])],
                                                          MARGIN = 1 ,
                                                          FUN = paste , collapse = ""),
                                      stringsAsFactors = FALSE)
          }
          if(length(cat_index_M)==1){
              field_code = data.frame(field_code = ac_property[ac_property$PROPNUM == x, lookup_match[1,2]],
                                      stringsAsFactors = FALSE)
          }

          if(is.element(field_code, field_match$field_match)){
              values = data.frame(t(lookup_tab[lookup_tab$field_match == field_code[1,1], seq(3,2+ncol(index))]),
                                  stringsAsFactors = FALSE)
              for(k in seq(1, ncol(index))){
                  lookup_parms_add[index[2,k], index[1,k]] = as.character(values[k,1])
              }

              lookup_parms_add = cbind(data.frame(rep(x, nrow(lookup_items)),
                                                  stringsAsFactors = FALSE),
                                      rep(economics_by_section[1,"SECTION"], nrow(lookup_items)),
                                      seq(1,nrow(lookup_items)),
                                      data.frame(rep(economics_by_section[economics_by_section$PROPNUM == x, "QUALIFIER"][1], nrow(lookup_items)),
                                                stringsAsFactors = FALSE),
                                      lookup_items,
                                      dplyr::bind_rows(lapply(seq(1, nrow(lookup_items)),
                                                      FUN = function(x){data.frame(paste(lookup_parms_add[x,], collapse = " "),stringsAsFactors = FALSE)}
                                      )),
                                      Rank = data.frame(rep(NA, nrow(lookup_items)),
                                                        stringsAsFactors = FALSE))

              names(lookup_parms_add) = names(economics_by_section)
          }

          return(lookup_parms_add)
        }))


      return(FINAL)
    }))

  return(FINAL_Inputs)
}

#' Convert Aries Sidefiles Tables to Hard Coded Lines (Excludes Price Files)
#'
#' @param economics_by_section Dataframe object containing economic lines for each Aries econonmic section.
#' @param ar_sidefile Dataframe containing the sidefile tables from an Aries database.
#' @param ac_property Dataframe containing the Master table from and Aries database.
#' @return Returns a dataframe object containing hard coded economic lines from Aries.
#' @examples
#' Sidefile_to_Hard_Coded(economics_by_section, ar_sidefile, AC_Property)
#' @description
#' This function will convert Aries sidefile tables within the Economics_by Section dataframe object into hard coded lines. (Excludes Price Files)
Sidefile_to_Hard_Coded = function(economics_by_section, ar_sidefile, ac_property){

  Hard_Coded_Inputs = bind_rows(lapply(X = unique(economics_by_section[economics_by_section$KEYWORD == "SIDEFILE", "EXPRESSION"]),
                                      FUN = function(i){
        sidefile = ar_sidefile[ar_sidefile$FILENAME == strsplit(i, " ")[[1]][1], ]
        # If the sidefile contains lines for a price forecast
        if(length(grep(pattern = "PRI", sidefile[,"KEYWORD"])) == 0){
          # If continuation lines exists, then paste the keyword from the previous non coninuation line.
          if(is.element("\"", sidefile$KEYWORD)){
              for(j in seq(1,length(sidefile$KEYWORD))){
                  if(sidefile[j,"KEYWORD"] == "\""){sidefile[j,"KEYWORD"] = sidefile[j-1,"KEYWORD"]}
              }
          }
          additional_lines = bind_rows(lapply(X = unique(economics_by_section[economics_by_section$EXPRESSION == i, "PROPNUM"]),
                                            FUN = function(x){
               sidefile_hard_code = cbind(data.frame(rep(x, nrow(sidefile)),
                                                      stringsAsFactors = FALSE),
                                          rep(economics_by_section[1,"SECTION"], nrow(sidefile)),
                                          seq(1, nrow(sidefile)),
                                          data.frame(rep(economics_by_section[economics_by_section$PROPNUM == x, "QUALIFIER"][1], nrow(sidefile)),
                                                    stringsAsFactors = FALSE),
                                          sidefile$KEYWORD,
                                          sidefile$EXPRESSION,
                                          Rank = data.frame(rep(NA, nrow(sidefile)),
                                                          stringsAsFactors = FALSE))
                return(sidefile_hard_code)
          }))
          names(additional_lines) = names(economics_by_section)
          return(additional_lines)
        }
  })
  )
  return(data.frame(Hard_Coded_Inputs, stringsAsFactors = FALSE))
}


#' Queries Aries Input Settings
#'
#' @param user_input_settings String containing the name of the Input Settings in Aries.
#' @param server String containing the server name where the Aries database is located.
#' @param database String containing the Aries database name.
#' @return Returns a list object containing the As of Date, Base Date, PW table, common lines, and default lines for a specified user input settings.
#' @examples
#' get_Input_Settings(user_input_settings = "HEC0119", server = "extsql01", database = "Aries")
#' @description
#' This function will retrieve the As of Date, Base Date, PW table, common lines, and default lines for the specified user input settings.
get_Input_Settings = function(user_input_settings, server, database){

    db_handle = get_dbhandle(server = server, database = database)
    ac_setup = sqlQuery(channel = db_handle, query = paste("SELECT * FROM AC_SETUP;"))
    ac_setupdata = sqlQuery(channel = db_handle, query = paste("SELECT * FROM AC_SETUPDATA;"))

    ac_setup = ac_setup[ac_setup$SETUPNAME == user_input_settings, ]

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

    return(list(as_of_date, base_date, pw_table, common_lines, default_lines))

}

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
#' @param well_comp_codes A string.
#' @return A dataframe containing Production data
#' @examples
#' get_Enertia_Production("49.1016.0022.00")
#' @description
#' The following functin will make a default connection to the Enertia_Reports database located on the Default Enertia04 Server.
#' It will then take the well_comp_codes vector which contains a sequence of Enertia Completion Codes to query.
#' The function will then return a dataframe object with all the enertia production data that exists for each
#' Enertia Completion Code within the well_comp_codes vector.
get_Enertia_Production = function(well_comp_codes){
    #Connect to Datasource
    db_handle = get_dbhandle(server = "Enertia04", database = "Enertia_Reports")
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
                             WHERE pdRptWellCompletionAssetTeamHierarchy.WellCompCode In (", paste0("'", well_comp_codes, collapse ="',") ,"')
                               AND pdMasProdAllocDetail.DtlProdDisp = 'FRM';")
    # Query the database, trim any excess white space, convert all columns to character type, adjust producttype to PROPER to prevent issues due to
    # case sensitivty when pvitoting table and convert the volumes to numeric.
    enertia_production = data.frame(RODBC::sqlQuery(db_handle, query))
    enertia_production[] = lapply(enertia_production, trimws)
    enertia_production[] = lapply(enertia_production, as.character)
    enertia_production[,"DtlVolume"] = as.numeric(enertia_production[,"DtlVolume"])
    enertia_production[, "DtlProdCode"] = toupper(enertia_production[, "DtlProdCode"])
    # Close ODBC data connection to free up datasource.
    RODBC::odbcClose(channel = db_handle)

    # PIVOT table by production code
    enertia_production = tryCatch(reshape2::dcast(data = enertia_production,
                                                  formula = WellCompCode+DtlProdDate~DtlProdCode,
                                                  value.var = "DtlVolume",
                                                  fun.aggregate = sum),
                         error = function(cond){
                                   print("ERROR: Data Not Found. Make sure the enertia entity exists.")
                                   enertia_production = NULL

                                 }
                         )

    if(!is.null(enertia_production)){
        # Replace all NA values within product columns with a 0
        enertia_production[is.na(enertia_production)] = 0
        #Add missing columns with 0 if data does'nt exist
        if(!is.element("GAS", names(enertia_production))){enertia_production = dplyr::bind_cols(enertia_production, GAS = rep(0, nrow(enertia_production)))}
        if(!is.element("OIL", names(enertia_production))){enertia_production = dplyr::bind_cols(enertia_production, OIL = rep(0, nrow(enertia_production)))}
        if(!is.element("WATER", names(enertia_production))){enertia_production = dplyr::bind_cols(enertia_production, WATER = rep(0, nrow(enertia_production)))}
        if(!is.element("PPROD", names(enertia_production))){enertia_production = dplyr::bind_cols(enertia_production, PPROD = rep(0, nrow(enertia_production)))}

        #Rearrange columns to maintain consistent table structure upon return.
        enertia_production = data.frame(Enertia_Code = enertia_production[ ,"WellCompCode"],
                                        Date = enertia_production[ ,"DtlProdDate"],
                                        OIL = enertia_production[ ,"OIL"],
                                        GAS = enertia_production[ ,"GAS"],
                                        PPROD = enertia_production[ ,"PPROD"],
                                        WATER = enertia_production[ ,"WATER"], stringsAsFactors = FALSE)
        # Convert PPROD units from gallons to stb (42gal = 1stb)
        enertia_production$PPROD = enertia_production$PPROD/42

    }

  return(enertia_production)
}

#' Retrieve Enertia Lineage as a data frame.
#'
#' @param asset_team_code A string.
#' @return A dataframe containing the Enertia Lineage of a set of Asset Team Codes
#' @examples
#' get_Enertia_Lineage_by_Asset_Team("RMR")
#' get_Enertia_Lineage_by_Asset_Team(c("RMR", "WLA", "ELA")
#' @description
#' The following functin will make a default connection to the Enertia_Reports database located on the Default Enertia04 Server.
#' It will then take the asset_team_code vector which contains a sequence of Enertia Asset Team Codes to query.
#' The function will then return a dataframe object with all enertia entities that exists for each
#' Asset Team Code within the asset_team_code vector.
get_Enertia_Lineage_by_Asset_Team = function(asset_team_code){

    db_handle_enertia04 = get_dbhandle(server = "ENERTIA04", database = "Enertia_Reports")
    db_handle_enertia01 = get_dbhandle(server = "ENERTIA01", database = "HEC_Enertia")
    #Compile Query
    query_enertia04 = paste0("SELECT pdRptWellCompletionAssetTeamHierarchy.WellCompCode,
                           pdRptWellCompletionAssetTeamHierarchy.WellCompName,
                           pdRptWellCompletionAssetTeamHierarchy.APINumber,
                           pdRptWellCompletionAssetTeamHierarchy.FieldName,
                           pdRptWellCompletionAssetTeamHierarchy.FieldCode,
                           pdRptWellCompletionAssetTeamHierarchy.SubFieldName,
                           pdRptWellCompletionAssetTeamHierarchy.SubFieldCode,
                           pdRptWellCompletionAssetTeamHierarchy.AssetTeamCode,
                           pdRptWellCompletionAssetTeamHierarchy.OperatorName,
                           pdRptWellCompletionAssetTeamHierarchy.State,
                           pdRptWellCompletionAssetTeamHierarchy.County
                           FROM pdRptWellCompletionAssetTeamHierarchy
                           WHERE pdRptWellCompletionAssetTeamHierarchy.AssetTeamCode In (", paste0("'", asset_team_code, collapse ="',") ,"');")

    query_enertia01 = paste0("SELECT hec_FbsLineage.WellCompCd,
                           hec_FbsLineage.WellboreCd,
                           hec_FbsLineage.WellboreNm,
                           hec_FbsLineage.AssetTeamCd
                           FROM hec_FbsLineage
                           WHERE hec_FbsLineage.AssetTeamCd In (", paste0("'", asset_team_code, collapse ="',") ,"');")


    # Query the database, trim any excess white space, convert all factor columns to character, and convert the volumes to numeric.
    enertia_lineage_04 = data.frame(RODBC::sqlQuery(db_handle_enertia04, query_enertia04))
    enertia_lineage_04[] = lapply(enertia_lineage_04, trimws)
    enertia_lineage_04[] = lapply(enertia_lineage_04, as.character)
    enertia_lineage_01 = data.frame(RODBC::sqlQuery(db_handle_enertia01, query_enertia01))
    enertia_lineage_01[] = lapply(enertia_lineage_01, trimws)
    enertia_lineage_01[] = lapply(enertia_lineage_01, as.character)
    # Close ODBC data connection to free up datasource.
    RODBC::odbcClose(channel = db_handle_enertia04)
    RODBC::odbcClose(channel = db_handle_enertia01)

    enertia_lineage = dplyr::inner_join(enertia_lineage_01, enertia_lineage_04, by = c('WellCompCd' = 'WellCompCode')) %>% data.frame(.,stringsAsFactors = FALSE)
    enertia_lineage = enertia_lineage[, c(2,3,1,5:14)]
    names(enertia_lineage) = c("WellBoreCode", "WellBoreName", "WellCompCode", "WellCompName", "APINumber", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "AssetTeamCode", "OperatorName", "State", "County")

    return(enertia_lineage)
}

#' Retrieve Enertia Lineage as a data frame.
#'
#' @param field_code A string.
#' @return A dataframe containing the Enertia Lineage of a set of Asset Team Codes
#' @examples
#' get_Enertia_Lineage_by_Field_Code("49.1017")
#' get_Enertia_Lineage_by_Field_Code(c("49.1017", "49.1015")
#' @description
#' The following functin will make a default connection to the Enertia_Reports database located on the Default Enertia04 Server.
#' It will then take the field_code vector which contains a sequence of Enertia Field Codes to query.
#' The function will then return a dataframe object with all enertia entities that exists for each
#' Field Code within the field_code vector.
get_Enertia_Lineage_by_Field_Code = function(field_code){

    db_handle_enertia04 = get_dbhandle(server = "ENERTIA04", database = "Enertia_Reports")
    db_handle_enertia01 = get_dbhandle(server = "ENERTIA01", database = "HEC_Enertia")

    #Compile Query
    query_enertia04 = paste0("SELECT pdRptWellCompletionAssetTeamHierarchy.WellCompCode,
                           pdRptWellCompletionAssetTeamHierarchy.WellCompName,
                           pdRptWellCompletionAssetTeamHierarchy.APINumber,
                           pdRptWellCompletionAssetTeamHierarchy.FieldName,
                           pdRptWellCompletionAssetTeamHierarchy.FieldCode,
                           pdRptWellCompletionAssetTeamHierarchy.SubFieldName,
                           pdRptWellCompletionAssetTeamHierarchy.SubFieldCode,
                           pdRptWellCompletionAssetTeamHierarchy.AssetTeamCode,
                           pdRptWellCompletionAssetTeamHierarchy.OperatorName,
                           pdRptWellCompletionAssetTeamHierarchy.State,
                           pdRptWellCompletionAssetTeamHierarchy.County
                           FROM pdRptWellCompletionAssetTeamHierarchy
                           WHERE pdRptWellCompletionAssetTeamHierarchy.FieldCode In (", paste0("'", field_code, collapse ="',") ,"');")

    query_enertia01 = paste0("SELECT hec_FbsLineage.WellCompCd,
                           hec_FbsLineage.WellboreCd,
                           hec_FbsLineage.WellboreNm,
                           hec_FbsLineage.FieldCd
                           FROM hec_FbsLineage
                           WHERE hec_FbsLineage.FieldCd In (", paste0("'", field_code, collapse ="',") ,"');")

    # Query the database, trim any excess white space, convert all factor columns to character, and convert the volumes to numeric.
    enertia_lineage_04 = data.frame(RODBC::sqlQuery(db_handle_enertia04, query_enertia04))
    enertia_lineage_04[] = lapply(enertia_lineage_04, trimws)
    enertia_lineage_04[] = lapply(enertia_lineage_04, as.character)
    enertia_lineage_01 = data.frame(RODBC::sqlQuery(db_handle_enertia01, query_enertia01))
    enertia_lineage_01[] = lapply(enertia_lineage_01, trimws)
    enertia_lineage_01[] = lapply(enertia_lineage_01, as.character)
    # Close ODBC data connection to free up datasource.
    RODBC::odbcClose(channel = db_handle_enertia04)
    RODBC::odbcClose(channel = db_handle_enertia01)

    enertia_lineage = dplyr::inner_join(enertia_lineage_01, enertia_lineage_04, by = c('WellCompCd' = 'WellCompCode')) %>% data.frame(.,stringsAsFactors = FALSE)
    enertia_lineage = enertia_lineage[, c(2,3,1,5:14)]
    names(enertia_lineage) = c("WellBoreCode", "WellBoreName", "WellCompCode", "WellCompName", "APINumber", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "AssetTeamCode", "OperatorName", "State", "County")


  return(enertia_lineage)
}

#' Retrieve Enertia Lineage as a data frame.
#'
#' @param enertia_code A string.
#' @return A dataframe containing the Enertia Lineage of a set of Asset Team Codes
#' @examples
#' get_Enertia_Lineage_by_Enertia_Code("49.1017.0007.00")
#' get_Enertia_Lineage_by_Enertia_Code(c("49.1017.0007.00", "49.1017.0023.00"))
#' @description
#' The following functin will make a default connection to the Enertia_Reports database located on the Default Enertia04 Server.
#' It will then take the enertia_code vector which contains a sequence of Enertia Codes to query.
#' The function will then return a dataframe object with all enertia entities that exists for each
#' Enertia Code within the enertia_code vector.
get_Enertia_Lineage_by_Enertia_Code = function(enertia_code){

    db_handle_enertia04 = get_dbhandle(server = "ENERTIA04", database = "Enertia_Reports")
    db_handle_enertia01 = get_dbhandle(server = "ENERTIA01", database = "HEC_Enertia")

    #Compile Query
    query_enertia04 = paste0("SELECT pdRptWellCompletionAssetTeamHierarchy.WellCompCode,
                           pdRptWellCompletionAssetTeamHierarchy.WellCompName,
                           pdRptWellCompletionAssetTeamHierarchy.APINumber,
                           pdRptWellCompletionAssetTeamHierarchy.FieldName,
                           pdRptWellCompletionAssetTeamHierarchy.FieldCode,
                           pdRptWellCompletionAssetTeamHierarchy.SubFieldName,
                           pdRptWellCompletionAssetTeamHierarchy.SubFieldCode,
                           pdRptWellCompletionAssetTeamHierarchy.AssetTeamCode,
                           pdRptWellCompletionAssetTeamHierarchy.OperatorName,
                           pdRptWellCompletionAssetTeamHierarchy.State,
                           pdRptWellCompletionAssetTeamHierarchy.County
                           FROM pdRptWellCompletionAssetTeamHierarchy
                           WHERE pdRptWellCompletionAssetTeamHierarchy.WellCompCode In (", paste0("'", enertia_code, collapse ="',") ,"');")

    query_enertia01 = paste0("SELECT hec_FbsLineage.WellCompCd,
                           hec_FbsLineage.WellboreCd,
                           hec_FbsLineage.WellboreNm
                           FROM hec_FbsLineage
                           WHERE hec_FbsLineage.WellCompCd In (", paste0("'", enertia_code, collapse ="',") ,"');")

    # Query the database, trim any excess white space, convert all factor columns to character, and convert the volumes to numeric.
    enertia_lineage_04 = data.frame(RODBC::sqlQuery(db_handle_enertia04, query_enertia04))
    enertia_lineage_04[] = lapply(enertia_lineage_04, trimws)
    enertia_lineage_04[] = lapply(enertia_lineage_04, as.character)
    enertia_lineage_01 = data.frame(RODBC::sqlQuery(db_handle_enertia01, query_enertia01))
    enertia_lineage_01[] = lapply(enertia_lineage_01, trimws)
    enertia_lineage_01[] = lapply(enertia_lineage_01, as.character)
    # Close ODBC data connection to free up datasource.
    RODBC::odbcClose(channel = db_handle_enertia04)
    RODBC::odbcClose(channel = db_handle_enertia01)

    enertia_lineage = dplyr::inner_join(enertia_lineage_01, enertia_lineage_04, by = c('WellCompCd' = 'WellCompCode'), keep = TRUE) %>% data.frame(.,stringsAsFactors = FALSE)
    enertia_lineage = enertia_lineage[, c(2,3,1,4:13)]
    names(enertia_lineage) = c("WellBoreCode", "WellBoreName", "WellCompCode", "WellCompName", "APINumber", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "AssetTeamCode", "OperatorName", "State", "County")


  return(enertia_lineage)
}


#' Retrieve Enertia Lineage as a data frame.
#'
#' @param API A string.
#' @return A dataframe containing the Enertia Lineage for a specified vector of API Numbers
#' @examples
#' get_Enertia_Lineage_by_API("5002921693")
#' get_Enertia_Lineage_by_API(c("5002921693", "50029219050000"))
#' @description
#' The following functin will make a default connection to the Enertia_Reports database located on the Default Enertia04 Server.
#' It will then take the API vector which contains a sequence of API Numbers to query.
#' The function will then return a dataframe object with all enertia entities that exists for each
#' API within the API vector.
get_Enertia_Lineage_by_API = function(API){

    db_handle_enertia04 = get_dbhandle(server = "ENERTIA04", database = "Enertia_Reports")
    db_handle_enertia01 = get_dbhandle(server = "ENERTIA01", database = "HEC_Enertia")

    enertia_lineage_04 = lapply(X = API, FUN = function(i){

        #Compile Query
        query_enertia04 = paste0("SELECT pdRptWellCompletionAssetTeamHierarchy.WellCompCode,
                                pdRptWellCompletionAssetTeamHierarchy.WellCompName,
                                pdRptWellCompletionAssetTeamHierarchy.APINumber,
                                pdRptWellCompletionAssetTeamHierarchy.FieldName,
                                pdRptWellCompletionAssetTeamHierarchy.FieldCode,
                                pdRptWellCompletionAssetTeamHierarchy.SubFieldName,
                                pdRptWellCompletionAssetTeamHierarchy.SubFieldCode,
                                pdRptWellCompletionAssetTeamHierarchy.AssetTeamCode,
                                pdRptWellCompletionAssetTeamHierarchy.OperatorName,
                                pdRptWellCompletionAssetTeamHierarchy.State,
                                pdRptWellCompletionAssetTeamHierarchy.County
                                FROM pdRptWellCompletionAssetTeamHierarchy
                                WHERE (((pdRptWellCompletionAssetTeamHierarchy.APINumber) Like '", paste0(i, "%'));"))


                            # Query the database, trim any excess white space, convert all factor columns to character, and convert the volumes to numeric.
                            enertia_lineage = data.frame(RODBC::sqlQuery(db_handle_enertia04, query_enertia04))
                            enertia_lineage[] = lapply(enertia_lineage, trimws)
                            enertia_lineage[] = lapply(enertia_lineage, as.character)

                          return(enertia_lineage)
      }) %>% dplyr::bind_rows(.)

    enertia_lineage_01 = lapply(X = API, FUN = function(i){

        #Compile Query
        query_enertia01 = paste0("SELECT hec_FbsLineage.WellCompCd,
                           hec_FbsLineage.WellboreCd,
                           hec_FbsLineage.WellboreNm
                           FROM hec_FbsLineage
                           WHERE hec_FbsLineage.WellCompCd In (", paste0("'", enertia_lineage_04$WellCompCode, collapse ="',") ,"');")


                            # Query the database, trim any excess white space, convert all factor columns to character, and convert the volumes to numeric.
                            enertia_lineage = data.frame(RODBC::sqlQuery(db_handle_enertia01, query_enertia01))
                            enertia_lineage[] = lapply(enertia_lineage, trimws)
                            enertia_lineage[] = lapply(enertia_lineage, as.character)

                          return(enertia_lineage)
     }) %>% dplyr::bind_rows(.)


    # Close ODBC data connection to free up datasource.
    RODBC::odbcClose(channel = db_handle_enertia04)
    RODBC::odbcClose(channel = db_handle_enertia01)

    enertia_lineage = dplyr::inner_join(enertia_lineage_01, enertia_lineage_04, by = c('WellCompCd' = 'WellCompCode'), keep = TRUE) %>% data.frame(.,stringsAsFactors = FALSE)
    enertia_lineage = enertia_lineage[, c(2,3,1,4:13)]
    names(enertia_lineage) = c("WellBoreCode", "WellBoreName", "WellCompCode", "WellCompName", "APINumber", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "AssetTeamCode", "OperatorName", "State", "County")



  return(enertia_lineage)
}

