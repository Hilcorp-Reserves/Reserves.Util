
##############################################  Tests for Create_Ac_Product  #########################################

WellCompCodes = RODBC::sqlQuery(channel = get_dbhandle(server = "Enertia01b", database = "HEC_Repository"),
                                query = paste0("SELECT WellCompCode FROM ofm_Lineage"))
WellCompCodes$WellCompCode = trimws(WellCompCodes$WellCompCode)

Propnum = RODBC::sqlQuery(channel = get_dbhandle(server = "extsql01", database = "Aries"),
                                query = paste0("SELECT PROPNUM FROM ariesadmin.AC_Property"))

x = sample(unique(WellCompCodes$WellCompCode), size = 100)
y = sample(unique(Propnum$PROPNUM), size = 100)

output_x = create_AC_Product_by_Link(enertia_codes = x)
output_y = create_AC_Product_by_Enertia_Code(propnum = y)

RODBC::odbcCloseAll()

test_that("The output is of type data.frame", {

  expect_true(is.element("data.frame", class(output_x)))
  expect_true(is.element("data.frame", class(output_y)))

})

test_that("The columns are ordered correctly", {

  expect_equal(object = names(output_x), expected = c("PROPNUM", "P_DATE", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ", "FTP", "WELL", "Overwrite", "ChangeDate", "ChangeUser"))
  expect_equal(object = names(output_y), expected = c("PROPNUM", "P_DATE", "OIL", "GAS", "WATER", "GAS_INJ", "WATER_INJ", "FTP", "WELL", "Overwrite", "ChangeDate", "ChangeUser"))

})

for (i in unique(output_x$PROPNUM)) {

    test_that("The output for each enertia code contains no more than 1 data point for each date.", {

      expect_equal(object = sum(duplicated(output_x[, c("PROPNUM", "P_DATE")])), expected = 0)

    })

}

for (i in unique(output_y$PROPNUM)) {

    test_that("The output for each enertia code contains no more than 1 data point for each date.", {

      expect_equal(object = sum(duplicated(output_y[, c("PROPNUM", "P_DATE")])), expected = 0)
      
    })

}
