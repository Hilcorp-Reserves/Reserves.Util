

########################################################  Tests for get_Economics  #################################################


scenario_name = c("FINAL0119_WF", "FINAL0119_JPM_HE1", "FINAL0119_HED_BNK", "FINAL0119_HE1_BNK",
                  "FINAL0119_STRIP_HE1", "FINAL0119_STRIP_HED", "FINAL0119_SEC_HED", "FINAL0119_SEC_HE1")
ac_property = RODBC::sqlFetch(channel = get_dbhandle(server = "extsql01", database = "Aries"), sqtable = "Ariesadmin.AC_PROPERTY")
ac_scenario = RODBC::sqlFetch(channel = get_dbhandle(server = "extsql01", database = "Aries"), sqtable = "Ariesadmin.AC_SCENARIO")
ac_scenario[] = lapply(ac_scenario, as.character)
RODBC::odbcCloseAll()

# Run test for each scenario.
for(i in scenario_name){

    #Sample economics for 1000 wells from ac_property table using the get_Economics function.
    economics_by_section = get_Economics(server = "extsql01",
                                        database = "Aries",
                                        PROPNUM = sample(ac_property$PROPNUM, size = 1000),
                                        scenario_name = i,
                                        view_by = "Scenario")
    scenario = ac_scenario[ac_scenario$SCEN_NAME == i, ]

    test_that("The Output contains a total of 7 elements", {

      expect_equal(object = length(economics_by_section), expected = 7)

    }) #Complete
    test_that("Each element of the output list is of type: data.frame", {

      expect_equal(object = class(economics_by_section[[1]]), expected = "data.frame")
      expect_equal(object = class(economics_by_section[[2]]), expected = "data.frame")
      expect_equal(object = class(economics_by_section[[3]]), expected = "data.frame")
      expect_equal(object = class(economics_by_section[[4]]), expected = "data.frame")
      expect_equal(object = class(economics_by_section[[5]]), expected = "data.frame")
      expect_equal(object = class(economics_by_section[[6]]), expected = "data.frame")
      expect_equal(object = class(economics_by_section[[7]]), expected = "data.frame")

    }) #Complete
    test_that("Each data.frame has the correct column names:", {

      expect_equal(object = sum(is.element(names(economics_by_section[[1]]),
                                            c("PROPNUM", "SECTION", "SEQUENCE", "QUALIFIER", "KEYWORD", "EXPRESSION", "Rank"))), expected = 7)
      expect_equal(object = sum(is.element(names(economics_by_section[[2]]),
                                            c("PROPNUM", "SECTION", "SEQUENCE", "QUALIFIER", "KEYWORD", "EXPRESSION", "Rank"))), expected = 7)
      expect_equal(object = sum(is.element(names(economics_by_section[[3]]),
                                            c("PROPNUM", "SECTION", "SEQUENCE", "QUALIFIER", "KEYWORD", "EXPRESSION", "Rank"))), expected = 7)
      expect_equal(object = sum(is.element(names(economics_by_section[[4]]),
                                            c("PROPNUM", "SECTION", "SEQUENCE", "QUALIFIER", "KEYWORD", "EXPRESSION", "Rank"))), expected = 7)
      expect_equal(object = sum(is.element(names(economics_by_section[[5]]),
                                            c("PROPNUM", "SECTION", "SEQUENCE", "QUALIFIER", "KEYWORD", "EXPRESSION", "Rank"))), expected = 7)
      expect_equal(object = sum(is.element(names(economics_by_section[[6]]),
                                            c("PROPNUM", "SECTION", "SEQUENCE", "QUALIFIER", "KEYWORD", "EXPRESSION", "Rank"))), expected = 7)
      expect_equal(object = sum(is.element(names(economics_by_section[[7]]),
                                            c("PROPNUM", "SECTION", "SEQUENCE", "QUALIFIER", "KEYWORD", "EXPRESSION", "Rank"))), expected = 7)

    }) #Complete
    test_that("Qualifiers in each section match qualifiers in the scenario", {

      scenario_sect_2_qualifiers = unique(as.character(scenario[2,4:13]))
      scenario_sect_4_qualifiers = unique(as.character(scenario[4,4:13]))
      scenario_sect_5_qualifiers = unique(as.character(scenario[5,4:13]))
      scenario_sect_6_qualifiers = unique(as.character(scenario[6,4:13]))
      scenario_sect_7_qualifiers = unique(as.character(scenario[7,4:13]))
      scenario_sect_8_qualifiers = unique(as.character(scenario[8,4:13]))
      scenario_sect_9_qualifiers = unique(as.character(scenario[9,4:13]))
      economics_sect_2_qualifiers = unique(economics_by_section[[1]]$QUALIFIER)
      economics_sect_4_qualifiers = unique(economics_by_section[[2]]$QUALIFIER)
      economics_sect_5_qualifiers = unique(economics_by_section[[3]]$QUALIFIER)
      economics_sect_6_qualifiers = unique(economics_by_section[[4]]$QUALIFIER)
      economics_sect_7_qualifiers = unique(economics_by_section[[5]]$QUALIFIER)
      economics_sect_8_qualifiers = unique(economics_by_section[[6]]$QUALIFIER)
      economics_sect_9_qualifiers = unique(economics_by_section[[7]]$QUALIFIER)

      expect_equal(object = all(is.element(economics_sect_2_qualifiers, scenario_sect_2_qualifiers) == TRUE), expected = TRUE)
      expect_equal(object = all(is.element(economics_sect_4_qualifiers, scenario_sect_4_qualifiers) == TRUE), expected = TRUE)
      expect_equal(object = all(is.element(economics_sect_5_qualifiers, scenario_sect_5_qualifiers) == TRUE), expected = TRUE)
      expect_equal(object = all(is.element(economics_sect_6_qualifiers, scenario_sect_6_qualifiers) == TRUE), expected = TRUE)
      expect_equal(object = all(is.element(economics_sect_7_qualifiers, scenario_sect_7_qualifiers) == TRUE), expected = TRUE)
      expect_equal(object = all(is.element(economics_sect_8_qualifiers, scenario_sect_8_qualifiers) == TRUE), expected = TRUE)
      expect_equal(object = all(is.element(economics_sect_9_qualifiers, scenario_sect_9_qualifiers) == TRUE), expected = TRUE)


    })
}




