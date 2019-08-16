
##############################################  Tests for get_Enertia_Production  #########################################

#Create ODBC Connection to sql server
dbhandle_enertia_01 = RODBC::odbcDriverConnect(paste0('driver={SQL Server};server=', "ENERTIA01", ';database=', "HEC_Enertia", ';trusted_connection=true'))
dbhandle_enertia_04 = RODBC::odbcDriverConnect(paste0('driver={SQL Server};server=', "ENERTIA04", ';database=', "Enertia_Reports", ';trusted_connection=true'))
asset_team = c("RMR", "WLA", "STX", "SJE")
field_code = c("49.1018", "42.1049", "50.0842", "30.1040")
completion_code = c("30.1037.0919.01", "42.0439.0538.00", "42.0687.0050.00", "42.0980.0003.00", "17.0430.0015.00")
API = c("4904120894", "4904120898", "4902322157")

test_that("The output is of type data.frame", {


  expect_equal(object = class(get_Enertia_Lineage_by_API(API = API)),
               expected = "data.frame")
  expect_equal(object = class(get_Enertia_Lineage_by_Asset_Team(asset_team_code = asset_team)),
               expected = "data.frame")
  expect_equal(object = class(get_Enertia_Lineage_by_Enertia_Code(enertia_code = completion_code)),
               expected = "data.frame")
  expect_equal(object = class(get_Enertia_Lineage_by_Field_Code(field_code = field_code)),
               expected = "data.frame")

})

test_that("The columns are ordered correctly", {

  expect_equal(object = names(get_Enertia_Lineage_by_API(API = API)),
               expected = c("WellBoreCode", "WellBoreName", "WellCompCode", "WellCompName", "APINumber", "Reservoir", "FieldName", "FieldCode", "SubFieldName",
                            "SubFieldCode", "AssetTeamCode", "OperatorName", "State", "County"))
  expect_equal(object = names(get_Enertia_Lineage_by_Asset_Team(asset_team_code = asset_team)),
               expected = c("WellBoreCode", "WellBoreName", "WellCompCode", "WellCompName", "APINumber", "Reservoir", "FieldName", "FieldCode", "SubFieldName",
                            "SubFieldCode", "AssetTeamCode", "OperatorName", "State", "County"))
  expect_equal(object = names(get_Enertia_Lineage_by_Enertia_Code(enertia_code = completion_code)),
               expected = c("WellBoreCode", "WellBoreName", "WellCompCode", "WellCompName", "APINumber", "Reservoir", "FieldName", "FieldCode", "SubFieldName",
                            "SubFieldCode", "AssetTeamCode", "OperatorName", "State", "County"))
  expect_equal(object = names(get_Enertia_Lineage_by_Field_Code(field_code = field_code)),
               expected = c("WellBoreCode", "WellBoreName", "WellCompCode", "WellCompName", "APINumber", "Reservoir", "FieldName", "FieldCode", "SubFieldName",
                            "SubFieldCode", "AssetTeamCode", "OperatorName", "State", "County"))

})



# Close all database connections prior to ending testing.
RODBC::odbcCloseAll()
