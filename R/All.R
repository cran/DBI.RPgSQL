## $Id: DBIPgSQL.R,v 1.3 2001/12/08 15:29:22 dj Exp dj $
##
## For simplicity, a DBI-PgSQL objects inherit an Id slot from the
## helper (mixin) class PgSQLObject  The first element drv@Id[1] is the
## process id of the current R session.
##
## RPgSQL currently allows one connection

require(DBI, quietly = TRUE, warn.conflicts = TRUE)

"RPgSQL" <- 
function()
## the the values for the various version slots could be
## discover at runtime.
{
   pid <- .Call("RS_getpid")
   new("PgSQLDriver", Id = newId(pid),
       driver.name = "RPgSQL",   driver.version = "1.0-0", DBI.version = "0.1-2",
       client.version= "NA", max.connections = 1)
}

## 
## Helper class "Id" (provides a mutable slot).
##

setClass("Id", representation(closure = "list"))
"newId" <- 
function(.Id = -1)
{
   cls <- list(getId = function() .Id, setId = function(id) .Id <<- id)
   new("Id", closure = cls)
}
setMethod("format", "Id",
   def = function(x, ...)
      paste("(", paste(x@closure$getId(), collapse=","), ")", sep="")
)
setMethod("print", "Id", 
   def = function(x, ...) print(format(x), quote=FALSE, ...)
)
## Simplify subsetting and replacement of Id objects
setMethod("[", "Id", 
   def = function(x, i, j, ..., drop = TRUE){
      if(missing(i)) 
         x@closure$getId() 
      else 
         x@closure$getId()[i]
   }
)
setReplaceMethod("[", "Id", 
   def = function(x, i, j, ..., value){
      out <- x@closure$getId()
      out[i] <- value
      x@closure$setId(out)
      x
   }
)

##
## Class: PgSQLObject
##
## This is a virtual class and groups all RPgSQL objects.  It also 
## provides a mutable slot "Id" to all RPgSQL objects to identify the 
## R session (needs to be mutable in order to indicate when an object 
## no longer refers to an active PgSQL driver/connection/result).

setClass("PgSQLObject", representation(Id = "Id", "DBIObject", "VIRTUAL"))
setMethod("format", "PgSQLObject", 
   def = function(x, ...) format(x@Id),
   valueClass = "character"
)
setMethod("print", "PgSQLObject",
   def = function(x, ...) {
      if(isIdCurrent(x))
         str <- paste("<", class(x), ":", format(x), ">", sep="")
      else 
         str <- paste("<Expired ",class(x), ":", format(x), ">", sep="")
      cat(str, "\n")
      invisible(NULL)
   }
)
setMethod("show", "PgSQLObject", def = function(object) print(object))
"isIdCurrent" <- 
function(obj)
## verify that obj refers to a currently open/loaded database
{ 
   id <- obj@Id[ ]   ## its length can be > 1
   pid <- .Call("RS_getpid")
   pid == id[1] && all(0<=id)
}

##
## Class: PgSQLDriver
##

setClass("PgSQLDriver", 
   rep = representation("DBIDriver", "PgSQLObject",
            driver.name = "character", driver.version = "character", 
            DBI.version = "character", client.version = "character", 
            max.connections = "numeric")
)
setMethod("dbGetInfo", "PgSQLDriver",
   def = function(dbObj, ...){
      ## return a list of slotname/value pairs
      if(!isIdCurrent(dbObj))
         stop(paste("Expired", class(dbObj), "object"))
      out <- list()
      for(sl in slotNames(dbObj)) 
         out[[sl]] <- slot(dbObj, sl)
      out
  }
)
setMethod("dbUnloadDriver", "PgSQLDriver", 
   def = function(drv, ...) { 
      if(isIdCurrent(drv))
         db.disconnect()
      drv@Id[1] <- -1
      TRUE
   }
)
setMethod("dbListConnections", "PgSQLDriver",
   def = function(drv, ...) new("PgSQLConnection", Id = newId(c(drv@Id[1],0)))
)

##
## Class: PgSQLConnection
##

setClass("PgSQLConnection", representation("DBIConnection", "PgSQLObject"))

setMethod("dbConnect", "PgSQLDriver",
   def = function(drv, ...){
      db.connect(...)
      new("PgSQLConnection", Id = newId(c(drv@Id[1], 0)))
   },
   valueClass = "PgSQLConnection"
)
## allow calls of the form dbConnect("RPgSQL", ...)
setMethod("dbConnect", "character",
   def = function(drv, ...){
      drv <- dbDriver(drv)
      dbConnect(drv, ...)
   },
   valueClass = "PgSQLConnection"
)
setMethod("dbGetQuery", 
   sig = signature(conn = "PgSQLConnection", statement = "character"),
   def = function(conn, statement, ...){
      db.execute(statement, ..., clear = F)
      db.fetch.result()
   },
   valueClass = "data.frame"
)
setMethod("dbGetInfo", "PgSQLConnection",
   def = function(dbObj, ...){
      list(dbname = db.name(), host = db.host.name(), user = db.user.name(),
           password = db.password(), status = db.connection.status(),
           open = db.connection.open(), options = db.connection.options(),
           debug.tty = db.debug.tty(), 
           server.version = "NA")
   },
   valueClass = "list"
)
setMethod("dbGetException", "PgSQLConnection",
   def = function(conn, ...) {
      ch <- conn@Id[2]
      list(errNum = NA, errMsg = db.error.message())
   },
   valueClass = "list"
)
setMethod("dbDisconnect", "PgSQLConnection", 
   def = function(conn, ...){
      rc <- try(db.disconnect())
      if(inherits(rc, "try-error"))
         return(FALSE)
      conn@Id[2] <- -1       ## mark the connection as expired
      TRUE
   },
   valueClass = "logical"
)
setMethod("dbSendQuery", 
   sig = signature(conn = "PgSQLConnection", statement = "character"),
   def = function(conn, statement, ...){
      db.execute(statement, ..., clear = F)
      new("PgSQLResult", Id = newId(c(conn@Id[1:2], 0)))
   },
   valueClass = "PgSQLResult"
)
setMethod("dbListTables", "PgSQLConnection",
   def = function(conn, ...){
      db.ls(...)
   },
   valueClass = "character"
)
setMethod("dbReadTable", 
   sig = signature(conn = "PgSQLConnection", name = "character"),
   def = function(conn, name, ...) {
      db.read.table(name, ...)
   },
   valueClass = "data.frame"
)
setMethod("dbWriteTable", 
   sig=signature(conn="PgSQLConnection",name="character",value="data.frame"),
   def = function(conn, name, value, ...) {
      rc <- try(db.write.table(value, name, ...))
      !inherits(rc, "try-error")
   },
   valueClass = "logical"
)
setMethod("dbExistsTable", 
   sig = signature(conn = "PgSQLConnection", name = "character"),
   def = function(conn, name, ...) {
      db.table.exists(name)
   },
   valueClass = "logical"
)
setMethod("dbRemoveTable", 
   sig = signature(conn="PgSQLConnection", name = "character"),
   def = function(conn, name, ...) {
      rc <- try(db.rm(name, ...))
      !inherits(rc, "try-error")
   },
   valueClass="logical"
)
setMethod("dbListResults", "RPgSQLConnection",
   def = function(conn, ...) .NotYetImplemented(),
   valueClass = "list"
)
setMethod("dbListFields", 
   sig = signature(conn="PgSQLConnection", name = "character"),
   def = function(conn, name, ...) .NotYetImplemented(),
   valueClass = "character"
)
setMethod("dbCommit", "PgSQLConnection",
   def = function(conn, ...) .NotYetImplemented()
)
setMethod("dbRollback", "PgSQLConnection",
   def = function(conn, ...) .NotYetImplemented()
)
setMethod("dbCallProc", "PgSQLConnection",
   def = function(conn, ...) .NotYetImplemented(),
   valueClass = "logical"
)

##
## Class: PgSQLResult
##

setClass("PgSQLResult", representation("DBIResult", "PgSQLObject"))

setMethod("dbGetInfo", "PgSQLResult",
   def = function(dbObj, ...){
      list(statement = dbGetStatement(dbObj),
           rows = db.result.rows(), cols = db.result.columns(),
           field.names = paste(db.result.column.names(), collapse=", "),
           hasCompleted = NA)
   },
   valueClass = "list"
)

setMethod("dbColumnInfo", "PgSQLResult",
   def = function(res, ...){
      ## compute data.frame w. col names, types, R types, and nullability
      pg.names <- db.result.column.names()
      pg.types <- rep(-1, length(pg.names))
      R.types <- rep("", length(pg.names))
      ## (from rpgsql.type.values methods)
      ty.pgsql <- c(1042,1043,1082,1083,1700,16,19,20,21,23,25,700,701)
      ty.Rtype <- c("factor", "factor", "dates", "times", "numeric",
                    "logical", "factor", "integer", "integer", 
                    "integer", "factor",  "numeric",  "numeric" )
      for(i in seq(along = pg.names)){
         pg.types[i] <- db.result.column.type(i)
         R.types[i] <- ty.Rtype[pg.types[i]==ty.pgsql]
      }
      data.frame(field.name = pg.names, field.type = pg.types, 
                 R.type = R.types, Nullable = NA, row.names = NULL)
   },
   valueClass = "data.frame"
)
setMethod("dbGetStatement", "PgSQLResult",
   def = function(res, ...) "",
   valueClass = "character"
)
setMethod("dbHasCompleted", "PgSQLResult",
   def = function(res, ...) .NotYetImplemented(),
   valueClass = "logical"
)
setMethod("dbGetException", "PgSQLResult",
   def = function(conn, ...){
      list(errNum = NA, errMsg = db.error.message())
   },
   valueClass = "list"
)
setMethod("fetch", "PgSQLResult",
   def = function(res, n = -1, ...) {
      if(n>0) warning("all available records are being fetched")
      db.fetch.result()
   },
   valueClass = "data.frame"
)
setMethod("dbClearResult", "PgSQLResult", 
   def = function(res, ...){
      rc <- try(db.clear.result())
      if(inherits(rc, "try-error"))
         return(FALSE)
      res@Id[3] <- -1    ## mark result as expired
      TRUE
   },
   valueClass = "logical"
)

## Utilities

setMethod("dbDataType", "PgSQLObject",
   def = function(dbObj, obj, ...){
      rpsql.data.type(obj)
   },
   valueClass = "character"
)
setMethod("make.db.names", "PgSQLObject",
   def = function(dbObj, snames, ...){
      gsub("\\.", "_", as.character(snames)) ## in-line RPgSQL's make.db.names
   },
   valueClass = "character"
)
## the following is a workaround to the problem of having
## make.db.names both on PgSQL and DBI
setMethod("make.db.names", signature(dbObj = "character", sname = "missing"),
   def = function(dbObj, snames, ...){
      snames <- dbObj
      gsub("\\.", "_", as.character(snames)) ## in-line RPgSQL's make.db.names
   },
   valueClass = "character"
)
setMethod("isSQLKeyword", "PgSQLObject",
   def = function(dbObj, name, ...) 
      isSQLKeyword.default(name, keywords = .SQL92Keywords, ...),
   valueClass = "logical"
)
".conflicts.OK" <- TRUE

".First.lib" <- 
function(lib, pkg) 
{
   library.dynam("DBI.RPgSQL", pkg, lib)
   require(RPgSQL, quietly = TRUE, warn.conflicts = FALSE)
   require(DBI, quietly = TRUE, warn.conflicts = FALSE) 
} 
