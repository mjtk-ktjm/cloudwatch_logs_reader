import pymysql
import os, sys, time, re
from queries.Loader import *


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
    cursor = db.cursor()
    cursor.execute("select requestId from "+tbl_prefix+"tbRequestDigest where  requestCt=20 || requestCt=16")
    results = cursor.fetchall()
except Exception as e:
    print("\n{}\n".format(e))
finally:
    db.close()

end_time = time.time()

print("Query took {} seconds.".format(int(end_time-start_time)))
num_results = len(results)
print(num_results)

db = pymysql.connect(host=db_host, 
    port=3306, 
    user=db_user, 
    passwd=db_pass, 
    db=db_name, 
    charset='utf8')

try:
    cursor = db.cursor()
except Exception as e:
    print("\n{}\n".format(e))
    db.close()
    sys.exit()

try:
    for result in results:
        sql = "INSERT INTO " + tbl_prefix + "tbLogCut " \
        + " (SELECT * FROM tbLogIngest " \
        + " WHERE requestId='{}' )".format(result[0]) 
        print(sql)
        cursor.execute(sql)
except Exception as e:
    print("\n{}\n".format(e))

db.commit()
db.close()
print('This is the END')

