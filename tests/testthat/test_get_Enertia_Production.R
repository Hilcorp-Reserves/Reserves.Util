
##############################################  Tests for get_Enertia_Production  #########################################

WellCompCodes = RODBC::sqlQuery(channel = get_dbhandle(server = "Enertia04", database = "Enertia_Reports"),
                                query = paste0("SELECT WellCompCode FROM pdRptWellCompletionAssetTeamHierarchy"))
WellCompCodes$WellCompCode = trimws(WellCompCodes$WellCompCode)
enertia_production = get_Enertia_Production(well_comp_codes = sample(x = unique(WellCompCodes$WellCompCode), size = 100))
RODBC::odbcCloseAll()

test_that("The output is of type data.frame", {

  expect_equal(object = class(enertia_production), expected = "data.frame")

})

test_that("The columns are ordered correctly", {

  expect_equal(object = names(enertia_production), expected = c("WellCompCode", "Date", "OIL", "GAS", "PPROD", "WATER"))

})

for (i in unique(enertia_production$Enertia_Code)) {

    test_that("The output for each enertia code contains no more than 1 data point for each date.", {

      expect_equal(object = sum(duplicated(enertia_production[, c("Enertia_Code", "Date")])), expected = 0)

    })

}


