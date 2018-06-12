

def etl_loader(_tbl_prefix, log_columns):

    _log_stream, _request_id, _datetime, _log_body = log_columns

    insert_query = "INSERT INTO `" + _tbl_prefix + "tbLogIngest` " \
    + "(logStream, requestId, datetime, logBody) " \
    + "VALUES ('{}','{}','{}','{}')".format(_log_stream, _request_id, _datetime, _log_body)
    # + "VALUES ('%s','%s','%s','%s')"

    return insert_query

