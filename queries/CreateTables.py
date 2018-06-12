

def sql_tblogingest(_tbl_prefix):
    #  The main cw log ingest table tracks these fields:
    #  request_id (varchar): cw logs request uuid
    #  datetime (varchar): datetime stamp of the log row
    #  seq (int): index of the log row in the context of request id
    #  log_body (text): whatever log comes after the request id in this log row

    create_query = "CREATE TABLE `" + _tbl_prefix + "tbLogIngest` " \
    + " (`pk` int not null auto_increment, " \
    + " `logStream` varchar(255) default NULL, " \
    + " `requestId` varchar(64) NOT NULL, " \
    + " `datetime` varchar(24) NOT NULL, " \
    + " `logBody` text NOT NULL, " \
    + " PRIMARY KEY(`pk`), " \
    + " Key `requestIdIndex` (`requestId`) )"
    
    return create_query


def sql_tblogcut(_tbl_prefix):

    create_query = "CREATE TABLE `" + _tbl_prefix + "tbLogCut` " \
    + " (`pk` int not null auto_increment, " \
    + " `logStream` varchar(255) default NULL, " \
    + " `requestId` varchar(64) NOT NULL, " \
    + " `datetime` varchar(24) NOT NULL, " \
    + " `logBody` text NOT NULL, " \
    + " PRIMARY KEY(`pk`), " \
    + " KEY `requestIdIndex` (`requestId`) )"
    
    return create_query

def sql_tbrequestdigest(_tbl_prefix):

    create_query = "CREATE TABLE `" + _tbl_prefix + "tbRequestDigest` " \
    + " (`pk` int not null auto_increment, " \
    + " `requestId` varchar(64) NOT NULL, " \
    + " `requestCt` int NOT NULL, " \
    + " PRIMARY KEY(`pk`) )"
    
    return create_query

def sql_tbmethodfocus(_tbl_prefix):

    create_query = "CREATE TABLE `tbMethodFocus` " \
    + " (`pk` int not null auto_increment, " \
    + " `requestId` varchar(64) NOT NULL, " \
    + " `datetime` varchar(24) NOT NULL, " \
    + " `methodRequest` varchar(24) NOT NULL, " \
    + " `resourcePath` varchar(24) NOT NULL, " \
    + " `methodRequestPath` varchar(24) NOT NULL, " \
    + " `queryString` varchar(24) NOT NULL, " \
    + " `` varchar(24) NOT NULL, " \
    + " PRIMARY KEY(`pk`) )"
    
    return create_query

def sql_tbcutbreakout(_tbl_prefix):

    create_query = "CREATE TABLE `" + _tbl_prefix + "tbCutBreakout` " \
    + " (`pk` int not null auto_increment, " \
    + " `requestId` varchar(64) NOT NULL, " \
    + " `startDatetime` varchar(24) NOT NULL, " \
    + " `duration` int NOT NULL, " \
    + " `apiStage` varchar(255) NOT NULL, " \
    + " `pathToken` varchar(255)  DEFAULT NULL, " \
    + " `queryLang` varchar(255)  DEFAULT NULL, " \
    + " `headerIfNoneMatch` varchar(255) DEFAULT NULL, " \
    + " `etag` varchar(255) DEFAULT NULL, " \
    + " `endpointResponseCode` int NOT NULL, " \
    + " `methodResponseCode` int NOT NULL, " \
    + " `isCacheHit` tinyint(1) default 0, " \
    + " PRIMARY KEY(`pk`), " \
    + " key `requestIdIndex` (`requestId`), " \
    + " key `isCacheHitKey` (`isCacheHit`) )"
    
    return create_query

