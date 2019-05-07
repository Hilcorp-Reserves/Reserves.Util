

##############################################  Tests for get_Enertia_Production  #########################################

WellCompCodes = RODBC::sqlQuery(channel = get_dbhandle(server = "Enertia04", database = "Enertia_Reports"),
                                query = paste0("SELECT WellCompCode FROM pdRptWellCompletionAssetTeamHierarchy"))
WellCompCodes$WellCompCode = trimws(WellCompCodes$WellCompCode)
enertia_production = get_Enertia_Production(WellCompCodes = sample(x = unique(WellCompCodes$WellCompCode), size = 100))
RODBC::odbcCloseAll()

test_that("The output is of type data.frame", {

  expect_equal(object = class(enertia_production), expected = "data.frame")

})

for (i in unique(enertia_production$WellCompCode)) {

    test_that("The output for each enertia code contains no more than 1 data point for each date.", {

      expect_equal(object = sum(duplicated(enertia_production[, c("WellCompCode", "DtlProdDate")])), expected = 0)

    })

}


