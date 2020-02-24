
##############################################  Tests for get_Enertia_Production  #########################################

WellCompCodes = RODBC::sqlQuery(channel = get_dbhandle(server = "Enertia01B", database = "HEC_Repository"),
                                query = paste0("SELECT WellCompCode FROM ofm_Lineage"))
WellCompCodes$WellCompCode = trimws(WellCompCodes$WellCompCode)
monthly_enertia_production = get_Enertia_Production(well_comp_codes = sample(x = unique(WellCompCodes$WellCompCode), size = 100))
daily_enertia_production = get_Daily_Enertia_Production((well_comp_codes = sample(x = unique(WellCompCodes$WellCompCode), size = 100)), months = 24)
RODBC::odbcCloseAll()

test_that("The output is of type data.frame", {

  expect_equal(object = class(monthly_enertia_production), expected = "data.frame")
  expect_equal(object = class(daily_enertia_production), expected = "data.frame")

})

test_that("The columns are ordered correctly", {

  expect_equal(object = names(monthly_enertia_production), expected = c("WellCompCode", "Date", "OIL", "GAS", "PPROD", "WATER", "GAS_INJ", "WATER_INJ"))
  expect_equal(object = names(daily_enertia_production), expected = c("WellCompCode", "Date", "OIL", "GAS", "PPROD", "WATER", "GAS_INJ", "WATER_INJ", "Oil_Sales", "Gas_Sales"))

})

for (i in unique(monthly_enertia_production$Enertia_Code)) {

    test_that("The output for each enertia code contains no more than 1 data point for each date.", {

      expect_equal(object = sum(duplicated(monthly_enertia_production[, c("WellCompCode", "Date")])), expected = 0)

    })

}

for (i in unique(daily_enertia_production$Enertia_Code)) {

    test_that("The output for each enertia code contains no more than 1 data point for each date.", {

      expect_equal(object = sum(duplicated(daily_enertia_production[, c("WellCompCode", "Date")])), expected = 0)
      
    })

}
