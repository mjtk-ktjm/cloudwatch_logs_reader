import pymysql
import os, sys, time, re
from queries.Loader import *

def generateBreakoutFromRecords(_tbl_prefix,request_rows):

    # TODO make this extendable to an arbitrary set of cache keys
    # AND lock down cache key source (path/query/header)

    _request_id = ''
    _start_datetime = ''
    _start_time = 0
    _duration = 0
    _api_stage = ''
    _path_token = ''
    _query_string_lang = ''
    _header_ifnonematch = ''
    _etag = ''
    _method_response_code = 0

    for row in request_rows:
        # set some high level metadata
        if _request_id == '':
            _request_id = row[2]
        if _start_time == 0:
            _start_time = int(row[5]) 
            _start_datetime = row[3]

        _duration = int(row[5]) - _start_time

        # for each row, grep out our deets
        search1 = re.compile('(?i)(API Stage: )([a-zA-Z0-9\/]+)')
        result1 = search1.search(row[4]) 
        if result1 is not None and len(result1[2])>0:
            _api_stage = result1[2]

        searcher = re.compile('(?i)(path: {token=)([a-zA-Z0-9/-]+)(})')
        result = searcher.search(row[4]) 
        if result is not None and len(result[2])>0:
            _path_token = result[2]

        searcher = re.compile('(?i)(query string: {lang=)([a-zA-Z0-9/-]+)(})')
        result = searcher.search(row[4]) 
        if result is not None and len(result[2])>0:
            _query_string_lang = result[2]

        searcher = re.compile('(?i)(If-None-Match=")([a-zA-Z0-9]+)(")')
        result = searcher.search(row[4]) 
        if result is not None and len(result[2])>0:
            _header_ifnonematch = result[2]

        searcher = re.compile('(?i)(ETag=")([a-zA-Z0-9]+)(")')
        result = searcher.search(row[4]) 
        if result is not None and len(result[2])>0:
            _etag = result[2]

        searcher = re.compile('(?i)(completed with status: )([0-9]+)')
        result = searcher.search(row[4]) 
        if result is not None and len(result[2])>0:
            _method_response_code = result[2]

        insert_sql = "INSERT INTO "+_tbl_prefix+"tbCutBreakout " \
        + "(requestId, startDatetime, startTime, duration, apiStage, pathToken, queryLang, headerIfNoneMatch, etag, methodResponseCode) " \
        + "VALUES ('{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', {}) " \
        .format(_request_id, _start_datetime, _start_time, _duration, _api_stage, _path_token, _query_string_lang, _header_ifnonematch, _etag, _method_response_code)

    return insert_sql


try:
    tbl_prefix = str(os.environ['TBLPREFIX']) + "_"
    db_host = os.environ['DBHOST']
    db_user = os.environ['DBUSERNAME']
    db_pass = os.environ['DBPASSWORD']
    db_name = os.environ['DBNAME']
except:
    print("This script requires TBLPREFIX, DBHOST, DBUSERNAME, DBPASSWORD, and DBNAME environment variables.")  
    sys.exit(1)

db = pymysql.connect(host=db_host, 
    port=3306, 
    user=db_user, 
    passwd=db_pass, 
    db=db_name, 
    charset='utf8')

start_time = time.time()

try:
    select_cursor = db.cursor()
    insert_cursor = db.cursor()
    select_cursor.execute("SELECT distinct(requestId) FROM "+tbl_prefix+"tbLogCut")
    distinct_requestIds = select_cursor.fetchall()
except Exception as e:
    print("\n{}\n".format(e))
    db.close()
    sys.exit()

end_time = time.time()

print("Query took {} seconds.".format(int(end_time-start_time)))
num_results = len(distinct_requestIds)
print(num_results)

i = 0

for requestId in distinct_requestIds:
    i += 1
    try:
        select_cursor.execute("SELECT *,unix_timestamp(datetime) as epoch_time FROM "+tbl_prefix+"tbLogCut WHERE requestId=%s ORDER BY epoch_time ASC", requestId[0])
        request_rows = select_cursor.fetchall()
        insert_breakout_sql = generateBreakoutFromRecords(tbl_prefix,request_rows)
        # print('\t{}'.format(insert_breakout_sql))
        insert_cursor.execute(insert_breakout_sql)
        db.commit()
    except Exception as e:
        print("\n{}\n".format(e))
        db.close()
        sys.exit()

db.close()
print('This is the END')

