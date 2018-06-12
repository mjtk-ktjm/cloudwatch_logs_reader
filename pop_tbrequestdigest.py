import pymysql
import os, sys, time, re
from queries.Loader import *
from pprint import pprint as pp


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
    cursor.execute("select count(*),requestId from "+tbl_prefix+"tbLogIngest group by requestId")
    results = cursor.fetchall()
except Exception as e:
    print("\n{}\n".format(e))
finally:
    db.close()

end_time = time.time()

print("Query took {} seconds.".format(int(end_time-start_time)))
num_results = len(results)

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

for result in results:
    print(result)
    sql = "INSERT INTO "+tbl_prefix+"tbRequestDigest " \
    + "(requestId, requestCt) " \
    + "VALUES ('{}', {})".format(result[1], result[0]) 
    cursor.execute(sql)
    print(sql)

db.commit()
db.close()
print('This is the END')

