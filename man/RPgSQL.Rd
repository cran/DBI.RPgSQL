\name{RPgSQL}
\alias{RPgSQL}
\title{Instantiate an R-PgSQL database driver}
\description{
  Instantiates an R-PgSQL database driver according
  to the common database interface (DBI).
}
\usage{
RPgSQL()
}
\details{
  The current implementation simply creates an object
  of class \code{PgSQLDriver} and attaches the
  package \code{RPgSQL}.
}
\value{
  An object of class \code{PgSQLDriver} that can be used
  to establish one or more simultaneous channels (connections) to one
  or more remote databases.
}

\note{This current version is simply a front-end in order to implement
  the common DBI on top of the existing (version 0.8-3) RPgSQL 
  package.  
}

\seealso{
  \link[DBI]{\code{dbDriver}},
  \link[DBI]{\code{dbConnect}},
  \link[DBI]{\code{dbSendQuery}},
  \link[DBI]{\code{dbListTables}},
  \link[RPgSQL]{\code{db.connect}}.
}

\examples{\dontrun{
drv <- dbDriver("RPgSQL")
con <- dbConnect(drv, "host", "user", "password", "dbname")
}
}
\keyword{interface}
\keyword{database} 

% vim: syntax=tex
