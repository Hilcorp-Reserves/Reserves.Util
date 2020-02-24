
##############################################  Tests for get_aries_enertia_links  #########################################

WellCompCodes = RODBC::sqlQuery(channel = get_dbhandle(server = "Enertia01B", database = "HEC_Repository"),
                                query = paste0("SELECT WellCompCode FROM ofm_Lineage"))
WellCompCodes$WellCompCode = trimws(WellCompCodes$WellCompCode)

links_by_area = get_Aries_Enertia_Links_by_Area(asset_team_code = "STX")
links_by_code = get_Aries_Enertia_Links_by_Enertia_Code(enertia_codes = c("42.0083.0001.00", "42.0680.0026.00", "42.0680.0139.00", "42.0684.0078.00", "42.0693.0251.00", "42.0683.0163.00"))

RODBC::odbcCloseAll()

test_that("The output is of type data.frame", {

  expect_equal(object = class(links_by_area), expected = "data.frame")
  expect_equal(object = class(links_by_code), expected = "data.frame")

})

test_that("The columns are ordered correctly", {

  expect_equal(object = names(links_by_area), expected = c("Seqnum", "WellCompCode", "WellCompName", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "FacPlatName", 
                                                                        "FacPlatCode", "APINumber", "OperatorName", "State", "County", "CreateUser", "CreateDate", "ChangeUser", "ChangeDate"))
  expect_equal(object = names(links_by_code), expected = c("Seqnum", "WellCompCode", "WellCompName", "FieldName", "FieldCode", "SubFieldName", "SubFieldCode", "FacPlatName", 
                                                           "FacPlatCode", "APINumber", "OperatorName", "State", "County", "CreateUser", "CreateDate", "ChangeUser", "ChangeDate"))

})


