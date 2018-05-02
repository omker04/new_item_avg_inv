# Title     : TODO
# Objective : TODO
# Created by: grp0009
# Created on: 27/04/17
errImpl <- function(v, message) {
  if (is.null(v) || v == "") {
    stop(message)
  }
}
dsImportImpl <- function(filePath, dataType) {
  if (dataType == "CSV") {
    read.csv(filePath)
  } else if (dataType == "TSV") {
    read.csv(filePath, sep="\t")
  } else if (dataType == "TABLE") {
    read.table(filePath)
  } else if (dataType == "XLS") {
    library(gdata)
    read.xls(filePath)
  } else if (dataType == "XLSX") {
    #Hack for file extension
    file.remove("/tmp/test.xlsx")
    file.copy(filePath, "/tmp/test.xlsx")
    readxl::read_excel("/tmp/test.xlsx")
  } else if (dataType == "JSON") {
    library(jsonlite)
    #fromJSON(filePath) %>% as.data.frame
    fromJSON(filePath)
  } else {
    stop(paste0("Unknown type: ", dataType))
  }
}
loadFile <- function(filePath, dataType) {
  if (dataType == "CSV") {
    read.csv(filePath)
  } else if (dataType == "TABLE") {
    read.table(filePath)
  } else if (dataType == "XLS") {
    library(gdata)
    read.xls(filePath)
  } else if (dataType == "XLSX") {
    library(gdata)
    read.xls(filePath)
  }
}
setEnvImpl <- function(envList, key, value) {
  v <- Sys.getenv(key)
  if (! is.null(v)) {
    envList[[key]] <- v
  }
  kv <- list()
  kv[[key]] <- value
  do.call(Sys.setenv, kv)
}
findHiveMetastoreHostImpl <- function(hiveSiteXML) {
  library('XML')
  doc <- xmlParse(hiveSiteXML)
  quorum_node <- getNodeSet(doc, "/configuration/property[name='hive.metastore.uris']")
  uris <- xmlValue(quorum_node[[1]][[2]])
  uri_splits <- strsplit(uris, ",", fixed=TRUE)
  return(strsplit(gsub("thrift://", "", uri_splits[[1]][1]), ":.*")[[c(1, 1)]])
}
findHiveMetastorePortImpl <- function(hiveSiteXML) {
  library('XML')
  doc <- xmlParse(hiveSiteXML)
  quorum_node <- getNodeSet(doc, "/configuration/property[name='hive.server2.thrift.port']")
  return(xmlValue(quorum_node[[1]][[2]]))
}
decryptPasswordImpl <- function(password, secretKey) {
  obj=.jnew("com.walmart.analytics.platform.datasets.encryption.EncryptionUtil")
  p <- .jcall(obj, "Ljava/lang/String;", "decryptData", password, secretKey)
  return(p)
}
decryptKeytabImpl <- function(enckeytab, secretKey) {
  obj=.jnew("com.walmart.analytics.platform.datasets.encryption.EncryptionUtil")
  p <- .jcall(obj, "[B", "decryptBinary", secretKey, enckeytab)
  return(p)
}
encryptFileImpl <- function(fileName, secretKey) {
  obj=.jnew("com.walmart.analytics.platform.datasets.encryption.EncryptionUtil")
  p <- .jcall(obj, "Ljava/lang/String;", "encryptFile", fileName, secretKey)
  return(p)
}
decryptFileImpl <- function(fileName, secretKey) {
  obj=.jnew("com.walmart.analytics.platform.datasets.encryption.EncryptionUtil")
  p <- .jcall(obj, "Ljava/lang/String;", "decryptFile", fileName, secretKey)
  return(p)
}
getConnectionStringImpl <- function(dbType, connection, secretKey, querySchema=NULL) {
  if (dbType == "TERADATA") {
    authContext <- connection$connection$connectionParams$authContext
    host <- connection$connection$connectionParams$inferredHostName
    schema <- connection$connection$connectionParams$schema
    if (! is.null(querySchema)) {
      schema <- querySchema
    }
    
    errImpl(host, "DB server host not set for DB data set")
    errImpl(authContext, "Auth-context not set for DB data set")
    authContext <- fromJSON(decryptPasswordImpl(authContext, secretKey))
    user <- authContext$username
    password <- authContext$password
    schemaOption = ""
    if (! is.null(schema)) {
      schemaOption = paste0(";Database=", schema)
    }
    
    return(paste0("Driver=Teradata;DBCName=", host, ";UID=", user, ";PWD={",
                  password, "};DataEncryption=Yes;MechanismName=LDAP",
                  schemaOption))
  } else if (dbType == "DB2") {
    authContext <- connection$connection$connectionParams$authContext
    host <- connection$connection$connectionParams$inferredHostName
    schema <- connection$connection$connectionParams$schema
    port <- "50000"
    errImpl(host, "DB server host not set for DB data set")
    errImpl(authContext, "Auth-context not set for DB data set")
    authContext <- fromJSON(decryptPasswordImpl(authContext, secretKey))
    user <- authContext$username
    password <- authContext$password
    
    #schemaOption = ";"
    #if (! is.null(schema)) {
    #    schemaOption = paste0(";Database=", schema)
    #}
    schemaOption = "/"
    if (! is.null(schema)) {
      schemaOption = paste0("/", schema)
    }
    if (! is.null(connection$connection$connectionParams$port)) {
      port <- connection$connection$connectionParams$port
    }
    
    #return(paste0("Driver=DB2;Hostname=", host, ";Port=", port, ";Protocol=TCPIP;Uid=",
    #user, ";Pwd=", password, schemaOption))
    return(paste0("jdbc:db2://", host, ":", port, schemaOption,
                  ":user=", user, ";password=", password, ";"))
  } else if (dbType == "SQLSERVER") {
    #Driver=TDS;Server=llbnt50009us.lab.wal-mart.com;Port=14481;UID=user;PWD=password;TDSVER=7.0
    authContext <- connection$connection$connectionParams$authContext
    host <- connection$connection$connectionParams$inferredHostName
    schema <- connection$connection$connectionParams$schema
    if (! is.null(querySchema)) {
      schema <- querySchema
    }
    
    port <- "1433"
    errImpl(host, "DB server host not set for DB data set")
    errImpl(authContext, "Auth-context not set for DB data set")
    authContext <- fromJSON(decryptPasswordImpl(authContext, secretKey))
    user <- authContext$username
    password <- authContext$password
    if (! is.null(connection$connection$connectionParams$port)) {
      port <- connection$connection$connectionParams$port
    }
    
    #schemaOption = ";"
    #if (! is.null(schema)) {
    #    schemaOption = paste0(";Database=", schema, ";")
    #}
    
    #return(paste0("Driver=SQLServer;Server=", host, ";Port=", port, ";UID=", user, ";PWD=", password, ";TDSVER=7.0", schemaOption))
    
    schemaOption = ";"
    if (! is.null(schema)) {
      schemaOption = paste0(schemaOption, "Database=", schema, ";")
    }
    if (length(grep("[\\]", user)) > 0) {
      domainName <- strsplit(user, "\\", fixed=TRUE)[[1]][1]
      userName <- substr(user, nchar(domainName) + 2, nchar(user))
      
      return(paste0("jdbc:jtds:sqlserver://", host, ":", port, schemaOption, "user=", userName,
                    ";password=", password, ";TDS=7.0;useNTLMv2=true;domain=", domainName))
    } else {
      return(paste0("jdbc:sqlserver://", host, ":", port, schemaOption, "userName=", user, ";password=", password))
    }
    
  } else if (dbType == "MYSQL") {
    authContext <- connection$connection$connectionParams$authContext
    host <- connection$connection$connectionParams$host
    schema <- connection$connection$connectionParams$schema
    errImpl(host, "DB server host not set for DB data set")
    errImpl(authContext, "Auth-context not set for DB data set")
    authContext <- fromJSON(decryptPasswordImpl(authContext, secretKey))
    user <- authContext$username
    password <- authContext$password
    
    schemaOption = ""
    if (! is.null(schema)) {
      schemaOption = paste0("Database=", schema, ";")
    }
    return(paste0("Driver=MySQL;Server=", host, ";Username=", user, ";Password=",
                  password, ";", schemaOption))
  } else if (dbType == "HIVE") {
    cluster <- connection$connection$connectionParams$cluster
    errImpl(cluster, "Cluster not set for Hive data set")
    hiveSiteXML <- paste0("/cluster_config/", cluster, "/conf/hive-site.xml")
    #host <- findHiveMetastoreHostImpl(hiveSiteXML)
    #port <- findHiveMetastorePortImpl(hiveSiteXML)
    hostPort <- connection$connection$connectionParams$inferredHostName
    hostPortSplit <- strsplit(hostPort, ":", fixed=TRUE)
    host <- hostPortSplit[[1]][1]
    port <- hostPortSplit[[1]][2]
    schema <- connection$connection$connectionParams$schema
    if (! is.null(querySchema)) {
      schema <- querySchema
    }
    
    schemaOption = ""
    if (! is.null(schema)) {
      schemaOption = paste0("Schema=", schema, ";")
    }
    return(paste0("Driver=Hive;HOST=", host, ";PORT=", port,
                  ";AuthMech=1;KrbRealm=", cluster, ".WAL-MART.COM;KrbHostFQDN=", host,
                  ";KrbServiceName=hive;HiveServerType=2;", schemaOption))
  } else {
    stop(paste0("Unknown DB type ", dbType))
  }
}

escapeSpecialCharsImpl <- function(password) {
  special_chars <- c("@", "\"")
  final_str <- ""
  password_chars <- strsplit(password, "")[[1]]
  for (ch in password_chars) {
    found = FALSE
    for (sch in special_chars) {
      if (ch == sch) {
        found = TRUE
        break
      }
    }
    if (found) {
      final_str <- paste0(final_str, "\\", ch)
    } else {
      final_str <- paste0(final_str, ch)
    }
  }
  return(final_str)
}
loadHdfsDataImpl <- function(connId, cluster, user, dataType, storageServiceHost,
                             storageServicePort, dataPath, secretKey, project, token, userId) {
  
  #Get keytab file
  envList <- list()
  url <- paste0("https://", storageServiceHost, ":", storageServicePort,
                "/v1/connections/", connId, "/keytab?deploymentType=project&deploymentId=",
                project, "&token=", token, "&userId=", userId)
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  r <- GET(url)
  stop_for_status(r)
  bin <- httr::content(r, "raw")
  keytabFile <- tempfile("tmp", tempdir(), "")
  cacheFile <- tempfile("tmp", tempdir(), "")
  writeBin(decryptKeytabImpl(bin, secretKey), keytabFile)
  
  obj <- .jnew("com.walmart.analytics.platform.datasets.connector.ProcessExecutor")
  dirPath <- .jcall(obj, "Ljava/lang/String;", "retrieveHDFSData", keytabFile, user,
                    cluster, dataPath, "")
  children <- list.files(path=dirPath, include.dirs=TRUE)
  if (length(children) == 0) {
    file.remove(dirPath)
    stop("Copying from HDFS failed!")
  } else if (length(children) > 1) {
    file.remove(dirPath)
    stop("Copying from HDFS created multiple files/directories in target directory")
  }
  fileName <- paste0(dirPath, "/", children[[1]])
  
  file.remove(keytabFile)
  df <- dsImportImpl(fileName, dataType)
  unlink(dirPath, recursive=TRUE)
  return(df)
}
executeHdfsCmdImpl <- function(connId, cluster, user, storageServiceHost,
                               storageServicePort, cmd, secretKey, project, token, userId) {
  
  #Get keytab file
  envList <- list()
  url <- paste0("https://", storageServiceHost, ":", storageServicePort,
                "/v1/connections/", connId, "/keytab?deploymentType=project&deploymentId=",
                project, "&token=", token, "&userId=", userId)
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  r <- GET(url)
  stop_for_status(r)
  bin <- httr::content(r, "raw")
  keytabFile <- tempfile("tmp", tempdir(), "")
  cacheFile <- tempfile("tmp", tempdir(), "")
  writeBin(decryptKeytabImpl(bin, secretKey), keytabFile)
  
  obj <- .jnew("com.walmart.analytics.platform.datasets.connector.ProcessExecutor")
  exec_res <- .jcall(obj, "Ljava/lang/String;", "executeHadoopCmd", keytabFile, user,
                     cluster, cmd, "")
  if (exec_res != "SUCCESS") {
    stop("Hadoop command execution failed")
  }
}
uploadToHDFSImpl <- function(connId, cluster, user, storageServiceHost,
                             storageServicePort, dataPath, hdfsPath, project, token, userId, secretKey) {
  url <- paste0("https://", storageServiceHost, ":", storageServicePort,
                "/v1/connections/", connId, "/keytab?deploymentType=project&deploymentId=",
                project, "&token=", token, "&userId=", userId)
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  r <- GET(url)
  stop_for_status(r)
  bin <- httr::content(r, "raw")
  keytabFile <- tempfile("tmp", tempdir(), "")
  writeBin(decryptKeytabImpl(bin, secretKey), keytabFile)
  
  obj <- .jnew("com.walmart.analytics.platform.datasets.connector.ProcessExecutor")
  copyStatus <- .jcall(obj, "Ljava/lang/String;", "uploadToHDFS", keytabFile, user,
                       cluster, dataPath, hdfsPath, "")
  if (copyStatus != "SUCCESS") {
    stop("Copy to hdfs failed")
  }
}
doAuthImpl <- function(dbType, connection, storageServiceHost, storageServicePort, envList,
                       secretKey, project, token, userId) {
  if (dbType == "HIVE") {
    authContext <- connection$connection$connectionParams$authContext
    cluster <- connection$connection$connectionParams$cluster
    errImpl(cluster, "Cluster not supplied for HDFS data set")
    authContext <- fromJSON(decryptPasswordImpl(authContext, secretKey))
    user <- authContext$username
    errImpl(user, "User not supplied for HDFS data set")
    
    url <- paste0("https://", storageServiceHost, ":", storageServicePort,
                  "/v1/connections/", connection$connection$id, "/keytab?deploymentType=project&deploymentId=",
                  project, "&token=", token, "&userId", userId)
    httr::set_config(httr::config(ssl_verifypeer = 0L))
    r <- GET(url)
    stop_for_status(r)
    bin <- httr::content(r, "raw")
    keytabFile <- tempfile("tmp", tempdir(), "")
    writeBin(decryptKeytabImpl(bin, secretKey), keytabFile)
    cacheFile <- tempfile("tmp", tempdir(), "")
    setEnvImpl(envList, "KRB5CCNAME", cacheFile)
    setEnvImpl(envList, "KRB5_CONFIG", paste0("/cluster_config/", cluster, "/conf/krb5.conf"))
    
    system(paste0("kinit -kt ", keytabFile, " -c ", cacheFile, " ", user,
                  "@", cluster, ".WAL-MART.COM"), intern=TRUE)
    file.remove(keytabFile)
    return(cacheFile)
  }
}
cleanupImpl <- function(cacheFile, envList) {
  if (! is.null(cacheFile)) {
    system("kdestroy", intern=TRUE)
    if (file.exists(cacheFile)) {
      file.remove(cacheFile)
    }
  }
  if (! is.null(envList) && length(envList) != 0) {
    do.call(Sys.setenv, envList)
  }
}
executeCmdImpl <- function(connection, statement = NULL,
                           datasetServiceHost = NULL, datasetServicePort = NULL, project = NULL,
                           secretKey = NULL, token = NULL, envName = NULL, userId = NULL) {
  dsType <- connection$connection$type
  if (dsType == "HDFS") {
    
    cluster <- connection$connection$connectionParams$cluster
    user <- connection$connection$connectionParams$username
    
    errImpl(cluster, "Cluster not supplied for HDFS data set")
    errImpl(user, "User not supplied for HDFS data set")
    
    executeHdfsCmdImpl(connection$connection$id, cluster, user,
                       datasetServiceHost, datasetServicePort, statement, secretKey, project, token, userId)
  } else if (dsType == "DB") {
    dbType <- connection$connection$connectionParams$flavor
    
    errImpl(dbType, "DB flavor not set for DB data set")
    if (dbType == "SQLSERVER") {
      library(RJDBC)
      cStr <- getConnectionStringImpl(dbType, connection, secretKey)
      if (length(grep("jdbc:sqlserver", cStr)) > 0) {
        drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", "/mssql-jdbc.jar")
      } else {
        drv <- JDBC("net.sourceforge.jtds.jdbc.Driver", "/jtds.jar")
      }
      
      conn <- dbConnect(drv, cStr)
      
      res <- dbSendUpdate(conn, statement)
      dbDisconnect(conn)
    } else if (dbType == "DB2") {
      library(RJDBC)
      cStr <- getConnectionStringImpl(dbType, connection, secretKey)
      drv <- JDBC("com.ibm.db2.jcc.DB2Driver", "/db2-jdbc.jar")
      conn <- dbConnect(drv, cStr)
      res <- dbSendUpdate(conn, statement)
      dbDisconnect(conn)
    } else {
      library(RODBC)
      
      odbcCloseAll()
      dbType <- connection$connection$connectionParams$flavor
      
      errImpl(dbType, "DB flavor not set for DB data set")
      
      envList <- list()
      cacheFile <- doAuthImpl(dbType, connection, datasetServiceHost, datasetServicePort,
                              envList, secretKey, project, token, userId)
      cStr <- getConnectionStringImpl(dbType, connection, secretKey)
      channel <- odbcDriverConnect(cStr, DBMSencoding="utf-8", believeNRows=FALSE)
      
      nulld <- sqlQuery(channel, statement)
      #cleanup
      cleanupImpl(cacheFile, envList)
      odbcCloseAll()
    }
  } else if (dsType == "SWIFT") {
    stop("Nothing to execute")
  }
}
dataLoadImpl <- function(connection, dataType = NULL, dataPath = NULL, query = NULL,
                         datasetServiceHost = NULL, datasetServicePort = NULL, project = NULL,
                         secretKey = NULL, token = NULL, envName = NULL, userId = NULL) {
  dsType <- connection$connection$type
  if (dsType == "HDFS") {
    
    cluster <- connection$connection$connectionParams$cluster
    user <- connection$connection$connectionParams$username
    
    errImpl(cluster, "Cluster not supplied for HDFS data set")
    errImpl(user, "User not supplied for HDFS data set")
    errImpl(dataType, "Data type not provided for HDFS data set")
    
    errImpl(dataPath, "Data path not provided for HDFS data set")
    
    loadHdfsDataImpl(connection$connection$id, cluster, user, toupper(dataType),
                     datasetServiceHost, datasetServicePort, dataPath, secretKey, project, token, userId)
  } else if (dsType == "DB") {
    dbType <- connection$connection$connectionParams$flavor
    
    errImpl(dbType, "DB flavor not set for DB data set")
    errImpl(query, "Query not supplied for DB data set")
    if (dbType == "SQLSERVER") {
      library(RJDBC)
      cStr <- getConnectionStringImpl(dbType, connection, secretKey)
      if (length(grep("jdbc:sqlserver", cStr)) > 0) {
        drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", "/mssql-jdbc.jar")
      } else {
        drv <- JDBC("net.sourceforge.jtds.jdbc.Driver", "/jtds.jar")
      }
      
      conn <- dbConnect(drv, cStr)
      df <- dbGetQuery(conn, query)
      dbDisconnect(conn)
      return(df)
    } else if (dbType == "DB2") {
      library(RJDBC)
      cStr <- getConnectionStringImpl(dbType, connection, secretKey)
      drv <- JDBC("com.ibm.db2.jcc.DB2Driver", "/db2-jdbc.jar")
      conn <- dbConnect(drv, cStr)
      df <- dbGetQuery(conn, query)
      dbDisconnect(conn)
      return(df)
    }
    library(RODBC)
    
    odbcCloseAll()
    dbType <- connection$connection$connectionParams$flavor
    
    errImpl(dbType, "DB flavor not set for DB data set")
    errImpl(query, "Query not supplied for DB data set")
    
    envList <- list()
    cacheFile <- doAuthImpl(dbType, connection, datasetServiceHost, datasetServicePort,
                            envList, secretKey, project, token, userId)
    cStr <- getConnectionStringImpl(dbType, connection, secretKey)
    channel <- odbcDriverConnect(cStr, DBMSencoding="utf-8", believeNRows=FALSE)
    
    dframe <- sqlQuery(channel, query)
    #cleanup
    cleanupImpl(cacheFile, envList)
    if (is.character(dframe)) {
      stop(paste(dframe, collapse = "\n"))
    }
    odbcCloseAll()
    return(dframe)
  } else if (dsType == "SWIFT") {
    
    authContext <- connection$connection$connectionParams$authContext
    objStoreTenant <- connection$connection$connectionParams$tenant
    objStoreRegion <- connection$connection$connectionParams$preferredRegion
    objStoreContainer <- connection$connection$connectionParams$container
    if (is.null(objStoreContainer)) {
      objStoreContainer <- paste0("mlp_container_", envName, "_", project)
    }
    objStoreAuthMethod <- connection$connection$connectionParams$authMethod
    
    errImpl(authContext, "Auth-Context not supplied for object store data set")
    errImpl(objStoreTenant, "Tenant not supplied for object store data set")
    errImpl(objStoreRegion, "Region not supplied for object store data set")
    errImpl(objStoreAuthMethod, "Auth-method not supplied for object store data set")
    errImpl(dataPath, "Data path not supplied for object store data set")
    errImpl(dataType, "Data type not supplied for object store data set")
    
    authContext <- fromJSON(decryptPasswordImpl(authContext, secretKey))
    objStoreAuthUrl <- authContext$authUrl
    objStoreUser <- authContext$username
    objStorePasswd <- authContext$password
    
    obj=.jnew("com.walmart.analytics.platform.datasets.objectstore.ObjectStoreClient")
    con=.jcall(obj, "Ljava/lang/String;", "downloadAsFile", objStoreAuthUrl, objStoreUser,
               objStoreTenant, objStorePasswd,
               objStoreRegion, objStoreAuthMethod,
               objStoreContainer, dataPath, dataType)
    #con <- textConnection(data)
    dsImportImpl(decryptFileImpl(con, secretKey), toupper(dataType))
  }
}
validateDataFormatImpl <- function(data_type) {
  if (data_type != "csv" && data_type != "json") {
    stop("Data can be stored in json or csv formats only")
  }
}

storeToHDFSImpl <- function(datasetServiceHost, datasetServicePort, name, project, userId, token, data_file, hdfs_path) {
  url <- paste0("https://", datasetServiceHost, ":", datasetServicePort,
                "/v1/connections?projectId=", project,
                "&name=", URLencode(name), "&deploymentType=project&deploymentId=",
                project, "&token=", token, "&userId=", userId)
  
  r <- GET(url)
  if (status_code(r) == 404) {
    stop("Connector with the given id/name doesn't exist")
  } else if (status_code(r) != 200) {
    stop(paste0("Failed querying for the connector: ", status_code(r)))
  }
  #stop_for_status(r)
  connection <- fromJSON(httr::content(r, "text"))
  cluster <- connection$connection$connectionParams$cluster
  user <- connection$connection$connectionParams$username
  secretKey <- connection$key
  
  errImpl(cluster, "Cluster not supplied for HDFS data set")
  errImpl(user, "User not supplied for HDFS data set")
  if (project != toString(connection$connection$project)) {
    stop("Accessing connectors from a different project is not allowed")
  }
  dsType <- connection$connection$type
  if (dsType != "HDFS") {
    stop("Connection not of HDFS type")
  }
  uploadToHDFSImpl(connection$connection$id, cluster, user, datasetServiceHost,
                   datasetServicePort, data_file, hdfs_path, project, token, userId, secretKey)
}
insertToDBImpl <- function(df, datasetServiceHost, datasetServicePort, name, project, userId, token, table) {
  url <- paste0("https://", datasetServiceHost, ":", datasetServicePort,
                "/v1/connections?projectId=", project,
                "&name=", URLencode(name), "&deploymentType=project&deploymentId=",
                project, "&token=", token, "&userId=", userId)
  
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  r <- GET(url)
  if (status_code(r) == 404) {
    stop("Connector with the given id/name doesn't exist")
  } else if (status_code(r) != 200) {
    stop(paste0("Failed querying for the connector: ", status_code(r)))
  }
  #stop_for_status(r)
  connection <- fromJSON(httr::content(r, "text"))
  if (project != toString(connection$connection$project)) {
    stop("Accessing connectors from a different project is not allowed")
  }
  dsType <- connection$connection$type
  if (dsType != "DB") {
    stop("Connection not of DB type")
  }
  library(DBI)
  library(odbc)
  
  dbType <- connection$connection$connectionParams$flavor
  secretKey <- connection$key
  
  errImpl(dbType, "DB flavor not set for DB data set")
  
  envList <- list()
  cacheFile <- doAuthImpl(dbType, connection, datasetServiceHost, datasetServicePort,
                          envList, secretKey, project, token, userId)
  
  schema <- NULL
  tableName <- table
  #if (length(grep("[.]", table)) > 0) {
  #    schema <- strsplit(table, ".", fixed=TRUE)[[1]][1]
  #    tableName <- substr(table, nchar(schema) + 2, nchar(table))
  #}
  cStr <- getConnectionStringImpl(dbType, connection, secretKey)
  if (dbType == "SQLSERVER") {
    library(RJDBC)
    if (length(grep("jdbc:sqlserver", cStr)) > 0) {
      drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", "/mssql-jdbc.jar")
    } else {
      drv <- JDBC("net.sourceforge.jtds.jdbc.Driver", "/jtds.jar")
    }
    channel <- dbConnect(drv, cStr)
  } else if (dbType == "DB2") {
    library(RJDBC)
    drv <- JDBC("com.ibm.db2.jcc.DB2Driver", "/db2-jdbc.jar")
    channel <- dbConnect(drv, cStr)
  } else {
    channel <- dbConnect(odbc::odbc(), .connection_string=cStr)
  }
  #channel <- odbcDriverConnect(cStr, DBMSencoding="utf-8", believeNRows=FALSE)
  
  if (dbType == "HIVE") {
    col_names <- names(df)
    colnames(df) <- sapply(col_names, tolower)
  }
  library(DBI)
  dbWriteTable(channel, tableName, df, append=TRUE)
  if (dbType == "HIVE") {
    colnames(df) <- col_names
  }
  
  cleanupImpl(cacheFile, envList)
}

#' Execute a command on a connector. The command could be a SQL query or a hadoop command
#'
#' Takes in a connector reference and executes a command. It finds the metadata
#' of the connection or dataset and uses that to connect to the actual source and
#' executes the command or sql query
#'
#' @param id Connector id
#' @param name Name of the connector. Either name or id is required to load data.
# If id is given, that's given preference over name.
#' @param statement SQL Query or hadoop command to execute
#'
#' @export
connector.execute <- function(id = NULL, name = NULL, statement= NULL) {
  if (is.null(statement)) {
    stop("DB statement or hadoop command must be supplied")
  }
  library('yaml')
  #datasetServiceHost <- Sys.getenv("DATASET_SERVICE_HOST")
  #datasetServicePort <- Sys.getenv("DATASET_SERVICE_PORT")
  #swiftJarFile <- Sys.getenv("SWIFT_CLIENT_JAR")
  
  library('rJava')
  swiftJarFile <- "/java-swift-library.jar"
  .jinit()
  .jaddClassPath(swiftJarFile)
  #project <- Sys.getenv("PROJECT")
  all_env <- yaml.load_file('/config.yaml')
  user_env <- yaml.load_file('/user.conf')
  ek <- file("/etc/secret/tokenkey/projectdecryptionkey", "r")
  d_key <- readLines(ek, n = 1, warn = FALSE)
  close(ek)
  all_env$PROJECT_TOKEN = decryptPasswordImpl(all_env$PROJECT_TOKEN, d_key)
  
  datasetServiceHost <- all_env$DATASET_SERVICE_HOST
  datasetServicePort <- all_env$DATASET_SERVICE_PORT
  #swiftJarFile <- all_env$SWIFT_CLIENT_JAR
  project <- all_env$PROJECT_ID
  token <- all_env$PROJECT_TOKEN
  envName <- tolower(all_env$ENV)
  if (envName == "stg") {
    envName = "stage"
  }
  userId <- user_env$USER_ID
  
  errImpl(datasetServiceHost, "Dataset service host is not set")
  errImpl(datasetServicePort, "Dataset service port is not set")
  errImpl(swiftJarFile, "Swift client jar file is not set")
  
  require("httr")
  require("jsonlite")
  
  url <- NULL
  if (! is.null(id)) {
    if (suppressWarnings(is.na(as.integer(id)))) {
      stop("id should be a number")
    }
    url <- paste0("https://", datasetServiceHost, ":", datasetServicePort,
                  "/v1/connections/", id, "?deploymentType=project&deploymentId=",
                  project, "&token=", token)
  } else if (! is.null(name)) {
    errImpl(project, "Project is not set")
    url <- paste0("https://", datasetServiceHost, ":", datasetServicePort,
                  "/v1/connections?projectId=", project,
                  "&name=", URLencode(name), "&deploymentType=project&deploymentId=",
                  project, "&token=", token, "&userId=", userId)
  } else {
    errImpl("Either connection id or name should be supplied")
  }
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  r <- GET(url)
  if (status_code(r) == 404) {
    stop("Connector with the given id/name doesn't exist")
  } else if (status_code(r) != 200) {
    stop(paste0("Failed querying for the connector: ", status_code(r)))
  }
  #stop_for_status(r)
  connection <- fromJSON(httr::content(r, "text"))
  if (project != toString(connection$connection$project)) {
    stop("Accessing connectors from a different project is not allowed")
  }
  executeCmdImpl(connection, statement,
                 datasetServiceHost, datasetServicePort, project, connection$key, token, envName, userId)
}

#' Load a data frame from dataset or connection reference
#'
#' Takes in a dataset or connection reference and loads a data frame. It finds the metadata
#' of the connection or dataset and uses that to connect to the actual source of data to
#' retrieve it and load it as a data frame
#'
#' @param id Dataset or connector id
#' @param name Name of the dataset or connector id. Either name or id is required to load data.
# If id is given, that's given preference over name.
#' @param path Path to hdfs. If path is provided, id or name is assumed to be that of a connector
#' @param query Hive or any database query. If query is provided, id or name is assumed to be that of a connector
#' @param data_type Type of data to load. This is a required attribute in case id or name is of an hdfs connector reference
#'
#' @return Data frame created from retrieved data
#' @export
dataset.load <- function(id = NULL, name = NULL, path = NULL, query = NULL, data_type = NULL) {
  library('yaml')
  #datasetServiceHost <- Sys.getenv("DATASET_SERVICE_HOST")
  #datasetServicePort <- Sys.getenv("DATASET_SERVICE_PORT")
  #swiftJarFile <- Sys.getenv("SWIFT_CLIENT_JAR")
  
  library('rJava')
  swiftJarFile <- "/java-swift-library.jar"
  .jinit()
  .jaddClassPath(swiftJarFile)
  #project <- Sys.getenv("PROJECT")
  all_env <- yaml.load_file('/config.yaml')
  user_env <- yaml.load_file('/user.conf')
  ek <- file("/etc/secret/tokenkey/projectdecryptionkey", "r")
  d_key <- readLines(ek, n = 1, warn = FALSE)
  close(ek)
  all_env$PROJECT_TOKEN = decryptPasswordImpl(all_env$PROJECT_TOKEN, d_key)
  
  datasetServiceHost <- all_env$DATASET_SERVICE_HOST
  datasetServicePort <- all_env$DATASET_SERVICE_PORT
  #swiftJarFile <- all_env$SWIFT_CLIENT_JAR
  project <- all_env$PROJECT_ID
  token <- all_env$PROJECT_TOKEN
  envName <- tolower(all_env$ENV)
  if (envName == "stg") {
    envName = "stage"
  }
  userId <- user_env$USER_ID
  
  errImpl(datasetServiceHost, "Dataset service host is not set")
  errImpl(datasetServicePort, "Dataset service port is not set")
  errImpl(swiftJarFile, "Swift client jar file is not set")
  
  require("httr")
  require("jsonlite")
  
  if (! is.null(path) | ! is.null(query)) {
    #If these are provided, we consider the id/name as connection id/name
    url <- NULL
    if (! is.null(id)) {
      if (suppressWarnings(is.na(as.integer(id)))) {
        stop("id should be a number")
      }
      url <- paste0("https://", datasetServiceHost, ":", datasetServicePort,
                    "/v1/connections/", id, "?deploymentType=project&deploymentId=",
                    project, "&token=", token)
    } else if (! is.null(name)) {
      errImpl(project, "Project is not set")
      url <- paste0("https://", datasetServiceHost, ":", datasetServicePort,
                    "/v1/connections?projectId=", project,
                    "&name=", URLencode(name), "&deploymentType=project&deploymentId=",
                    project, "&token=", token, "&userId=", userId)
    } else {
      errImpl("Either connection id or name should be supplied")
    }
    httr::set_config(httr::config(ssl_verifypeer = 0L))
    r <- GET(url)
    if (status_code(r) == 404) {
      stop("Connector with the given id/name doesn't exist")
    } else if (status_code(r) != 200) {
      stop(paste0("Failed querying for the connector: ", status_code(r)))
    }
    #stop_for_status(r)
    connection <- fromJSON(httr::content(r, "text"))
    if (project != toString(connection$connection$project)) {
      stop("Accessing connectors from a different project is not allowed")
    }
    dataLoadImpl(connection, data_type, path, query,
                 datasetServiceHost, datasetServicePort, project, connection$key, token, envName, userId)
  } else {
    if (! is.null(id)) {
      if (suppressWarnings(is.na(as.integer(id)))) {
        stop("id should be a number")
      }
      url <- paste0("https://", datasetServiceHost, ":", datasetServicePort, "/v1/datasets/",
                    id, "?deploymentType=project&deploymentId=", project, "&token=", token, "&userId=", userId)
    } else if (! is.null(name)) {
      errImpl(project, "Project is not set")
      url <- paste0("https://", datasetServiceHost, ":", datasetServicePort, "/v1/datasets?name=",
                    URLencode(name), "&projectId=", project, "&deploymentType=project&deploymentId=",
                    project, "&token=", token, "&userId=", userId)
    } else {
      errImpl("Either dataset id or name should be supplied")
    }
    httr::set_config(httr::config(ssl_verifypeer = 0L))
    r <- GET(url)
    if (status_code(r) == 404) {
      stop("Dataset with the given id/name doesn't exist")
    } else if (status_code(r) != 200) {
      stop(paste0("Failed querying for the dataset: ", status_code(r)))
    }
    #stop_for_status(r)
    df <- fromJSON(httr::content(r, "text"))
    if (project != toString(df$dataset$project)) {
      stop("Accessing datasets from a different project is not allowed")
    }
    dataLoadImpl(df$dataset, df$dataset$type, df$dataset$params$path, df$dataset$params$query,
                 datasetServiceHost, datasetServicePort, project, df$key, token, envName, userId)
  }
}

#' Store data frame as a dataset
#'
#' Takes a data frame and stores it as a dataset. The data will be stored in ML platform's storage. The function
#' returns the id of the dataset created.
#'
#' @param df Data frame
#' @param name Name of the dataset to be created
#' @param description Description of the dataset
#' @param over_write Whether to over_write if the dataset is already existing. FALSE by default.
#' @param data_type Format in which data should be stored. Default is CSV.
#'
#' @return ID of the dataset created
#' @export
dataset.save <- function(df, name, description=NULL, over_write=FALSE, data_type="csv",
                         table_name=NULL, hdfs_path=NULL) {
  if (is.null(data_type)) {
    data_type = "csv"
  } else {
    data_type = tolower(data_type)
  }
  validateDataFormatImpl(data_type)
  library('yaml')
  all_env <- yaml.load_file('/config.yaml')
  user_env <- yaml.load_file('/user.conf')
  
  library('rJava')
  swiftJarFile <- "/java-swift-library.jar"
  .jinit()
  .jaddClassPath(swiftJarFile)
  ek <- file("/etc/secret/tokenkey/projectdecryptionkey", "r")
  d_key = readLines(ek, n = 1, warn = FALSE)
  close(ek)
  all_env$PROJECT_TOKEN = decryptPasswordImpl(all_env$PROJECT_TOKEN, d_key)
  
  datasetServiceHost <- all_env$DATASET_SERVICE_HOST
  datasetServicePort <- all_env$DATASET_SERVICE_PORT
  project <- all_env$PROJECT_ID
  userId <- user_env$USER_ID
  token <- all_env$PROJECT_TOKEN
  envName <- tolower(all_env$ENV)
  if (envName == "stg") {
    envName = "stage"
  }
  
  errImpl(datasetServiceHost, "Dataset service host is not set")
  errImpl(datasetServicePort, "Dataset service port is not set")
  errImpl(project, "Project id is not set")
  
  require("httr")
  require("jsonlite")
  if (! is.null(table_name)) {
    insertToDBImpl(df, datasetServiceHost, datasetServicePort, name, project, userId, token, table_name)
    return(0)
  }
  library('uuid')
  fileName <- tempfile("tmp", tempdir(), "")
  if (data_type == "json") {
    write_json(df, fileName, dataframe="rows")
  } else {
    write.csv(df, fileName, row.names = FALSE)
  }
  if (! is.null(hdfs_path)) {
    storeToHDFSImpl(datasetServiceHost, datasetServicePort, name, project, userId, token, fileName, hdfs_path)
    return(0)
  }
  
  #header_row <- paste(names(df), collapse=", ")
  #tab <- apply(df, 1, function(x)paste(x, collapse=", "))
  #text <- paste(c(header_row, tab), collapse="\n")
  
  #Check if a dataset by name exists
  url <- paste0("https://", datasetServiceHost, ":", datasetServicePort, "/v1/datasets?name=",
                URLencode(name), "&projectId=", project, "&deploymentType=project&deploymentId=",
                project, "&token=", token, "&userId=", userId)
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  r <- GET(url)
  requestType <- "POST"
  isEdit <- FALSE
  if (status_code(r) == 200 && ! over_write) {
    stop("Dataset by the same name exists")
  } else if (status_code(r) == 200) {
    requestType <- "POST"
    isEdit <- TRUE
  }
  
  url <- paste0("https://", datasetServiceHost, ":", datasetServicePort,
                "/v1/connections?name=default_connection&currentProjectId=", project,
                "&deploymentType=project&deploymentId=", project, "&token=", token, "&userId=", userId)
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  r <- GET(url)
  if (status_code(r) != 200) {
    stop(paste0("Failed querying for connector: ", status_code(r)))
  }
  #stop_for_status(r)
  connection <- fromJSON(httr::content(r, "text"))
  dataPath = paste0(UUIDgenerate(), ".", data_type)
  
  connId <- connection$connection$id
  authContext <- connection$connection$connectionParams$authContext
  objStoreTenant <- connection$connection$connectionParams$tenant
  objStoreRegion <- connection$connection$connectionParams$preferredRegion
  objStoreContainer <- connection$connection$connectionParams$container
  if (is.null(objStoreContainer)) {
    objStoreContainer <- paste0("mlp_container_", envName, "_", project)
  }
  objStoreAuthMethod <- connection$connection$connectionParams$authMethod
  projectSecret <- connection$key
  
  errImpl(authContext, "Auth-Context not supplied for object store data set")
  errImpl(objStoreTenant, "Tenant not supplied for object store data set")
  errImpl(objStoreRegion, "Region not supplied for object store data set")
  errImpl(objStoreAuthMethod, "Auth-method not supplied for object store data set")
  errImpl(projectSecret, "Project secret not found")
  
  authContext <- fromJSON(decryptPasswordImpl(authContext, projectSecret))
  objStoreAuthUrl <- authContext$authUrl
  objStoreUser <- authContext$username
  objStorePasswd <- authContext$password
  
  encryptedFile <- encryptFileImpl(fileName, projectSecret)
  
  obj=.jnew("com.walmart.analytics.platform.datasets.objectstore.ObjectStoreClient")
  .jcall(obj, "V", "uploadContentFromFile", objStoreAuthUrl, objStoreUser, objStoreTenant,
         objStorePasswd, objStoreRegion, objStoreAuthMethod,
         objStoreContainer, dataPath, encryptedFile)
  #Need to create a DataSet
  url <- paste0("https://", datasetServiceHost, ":", datasetServicePort,
                "/v1/datasets")
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  if (isEdit) {
    url <- paste0(url, "/edit")
  }
  url <- paste0(url, "?deploymentType=project&deploymentId=", project, "&token=", token)
  if (isEdit) {
    reqBody <- list(connectionId=connId, projectId=project, type=toupper(data_type), name=name,
                    updatedBy=userId, parameters=list(path=dataPath))
  } else {
    reqBody <- list(connectionId=connId, projectId=project, type=toupper(data_type), name=name,
                    createdBy=userId, parameters=list(path=dataPath))
  }
  if (! is.null(description)) {
    reqBody$description <- description
  }
  x <- as.character(toJSON(reqBody, auto_unbox = TRUE))
  metadata <- curl:::form_data(x,"application/json")
  body <- list(dataset=metadata)
  
  handle <- curl::new_handle()
  req <- httr:::request(fields=body)
  req <- httr:::request_build(requestType,url,req)
  r <- httr:::request_perform(req,handle)
  
  #r <- POST(url, body = reqBody, encode = "json")
  stop_for_status(r)
  ds <- fromJSON(httr::content(r, "text"))
  return(ds$dataset$id)
}

#' List datasets present in the current project
#'
#' This function returns the list of datasets present in this project. Returned dictionary contains
#' the dataset name as key and id as value.
#'
#' @return List of dataset names and id
#' @export

dataset.list <- function() {
  library('yaml')
  all_env <- yaml.load_file('/config.yaml')
  user_env <- yaml.load_file('/user.conf')
  datasetServiceHost <- all_env$DATASET_SERVICE_HOST
  datasetServicePort <- all_env$DATASET_SERVICE_PORT
  project <- all_env$PROJECT_ID
  
  library('rJava')
  swiftJarFile <- "/java-swift-library.jar"
  .jinit()
  .jaddClassPath(swiftJarFile)
  
  ek <- file("/etc/secret/tokenkey/projectdecryptionkey", "r")
  d_key = readLines(ek, n = 1, warn = FALSE)
  close(ek)
  all_env$PROJECT_TOKEN = decryptPasswordImpl(all_env$PROJECT_TOKEN, d_key)
  
  token <- all_env$PROJECT_TOKEN
  userId <- user_env$USER_ID
  
  errImpl(datasetServiceHost, "Dataset service host is not set")
  errImpl(datasetServicePort, "Dataset service port is not set")
  errImpl(project, "Project id is not set")
  errImpl(userId, "User id is not set")
  
  require("httr")
  require("jsonlite")
  
  url <- paste0("https://", datasetServiceHost, ":", datasetServicePort, "/v1/datasets?projectId=",
                project, "&deploymentType=project&deploymentId=", project, "&token=", token, "&userId=", userId)
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  r <- GET(url)
  if (status_code(r) != 200) {
    stop(paste0("Failed querying for datasets: ", status_code(r)))
  }
  
  response <- fromJSON(httr::content(r, "text"))
  datasets <- response$datasets
  
  ids <- datasets$id
  names <- datasets$name
  ds_list <- list()
  for (i in 1:length(ids)) {
    ds_list[names[i]] = ids[i]
  }
  return(ds_list)
}
