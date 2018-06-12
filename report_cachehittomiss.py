import pymysql
import os, sys, time, re
from queries.Loader import *
from collections import defaultdict

def bucketize_log_rows(rows):
    bucketized = dict()
    log_floor = None
    log_ceil = 0
    has_miss = 0 
    has_hit = 0 

    for log_row in rows:
        if log_floor is None:
            remainder = log_row[0] % 300
            log_floor = log_row[0] - remainder
            next_floor = log_floor + 300
        else:
            if log_row[0] >= next_floor:
                bucketized[log_floor] = [has_miss, has_hit]
                log_floor = next_floor
                next_floor = log_floor + 300
                has_miss = 0
                has_hit = 0

        if has_miss == 0:
            has_miss += 1
        else:
            has_hit +=1
    return bucketized


try:
    tbl_prefix = str(os.environ['TBLPREFIX']) + "_"
    db_host = os.environ['DBHOST']
    db_user = os.environ['DBUSERNAME']
    db_pass = os.environ['DBPASSWORD']
    db_name = os.environ['DBNAME']
except:
    print("This script requires TBLPREFIX, DBHOST, DBUSERNAME, DBPASSWORD, and DBNAME environment variables.")  
    sys.exit(1)

aggregated_counts = {'hit': defaultdict(lambda: 0), 'miss': defaultdict(lambda:0)}

db = pymysql.connect(host=db_host, 
    port=3306, 
    user=db_user, 
    passwd=db_pass, 
    db=db_name, 
    charset='utf8')

start_time = time.time()

try:
    select_cursor = db.cursor()
    sql = "select count(*), pathToken, queryLang, headerIfNoneMatch " \
        + "FROM "+tbl_prefix+"tbCutBreakout " \
        + "GROUP BY pathToken, queryLang, headerIfNoneMatch " \
        + "ORDER BY count(*)"
    select_cursor.execute(sql)
    request_results = select_cursor.fetchall()
except Exception as e:
    print("\n{}\n".format(e))
    db.close()
    sys.exit()

end_time = time.time()

for fingerprint in request_results:
    _, path, query, header = fingerprint

    sql = "SELECT startTime,queryLang,etag,methodResponseCode " \
    + "FROM "+tbl_prefix+"tbCutBreakout " \
    + "WHERE pathToken='{}' ".format(path) \
    + " AND queryLang='{}' ".format(query) \
    + " AND headerIfNoneMatch='{}' ".format(header) \
    + "ORDER BY startTime ASC"

    try:
        select_cursor.execute(sql)
        fingerprint_results = select_cursor.fetchall()
        f_results = bucketize_log_rows(fingerprint_results)
    except Exception as e:
        print("\n{}\n".format(e))
        db.close()
        sys.exit()
    if len(fingerprint_results)<2:
        continue

    for f_result_key,f_result_val in f_results.items():
        aggregated_counts['hit'][f_result_key] += f_result_val[1]
        aggregated_counts['miss'][f_result_key] += f_result_val[0]

db.close()

for ts in aggregated_counts['hit'].keys():
    print("{} : {}/{}".format(ts,aggregated_counts['miss'][ts], aggregated_counts['hit'][ts]))

print('This is the END')

