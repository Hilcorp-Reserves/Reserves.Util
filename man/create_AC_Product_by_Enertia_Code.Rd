% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Reserves_Util.R
\name{create_AC_Product_by_Enertia_Code}
\alias{create_AC_Product_by_Enertia_Code}
\title{Create an AC_PRODUCT table consisting of enertia codes linked to the list of speicifed Aries propnum.}
\usage{
create_AC_Product_by_Enertia_Code(
  propnum,
  server = "extsql01",
  database = "Aries",
  user = Sys.getenv("USERNAME"),
  overwrite = "Y",
  export = FALSE
)
}
\arguments{
\item{propnum}{a character vector.}

\item{server}{a character vector.}

\item{database}{a character vector.}

\item{user}{a character vector.}

\item{overwrite}{a character vector.}

\item{export}{a boolean value}
}
\value{
A dataframe containing production data formated for the AC_PRODUCT table in Aries
}
\description{
The following functin will make a connection to an Aries database.
It will then take the user defined 'propnum' vector which contains one or more Aries Propnum to query.
The function will then return a dataframe object which contains enertia codes linked to the specified ariespropnum and the associated production data.
the data frame is returned in the same format as the Aries AC_PRODUCT table.
the export variable can be set equal to TRUE to export the output to the users desktop.
}
\examples{
create_AC_Product_by_Enertia_Code(enertia_codes = c("0WHEZJ4SQ4ZJ", "YXK78QADR9BZ"), user = "user01", overwrite = "N", export = TRUE)
}
