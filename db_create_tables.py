import pymysql
import os, sys
from datetime import date
import queries


# table prefix permits script to be run repeatedly with results confined to namespace
if 'TBLPREFIX' in os.environ.keys():
    tbl_prefix = str(os.environ['TBLPREFIX']) + "_"
else:
    today = date.today()
    tt = today.timetuple()
    tbl_prefix = "{}{}{}_".format(tt[0], tt[1], tt[2]) 
    print("Be sure to export TBLPREFIX to your ENV!")
    print("export TBLPREFIX="+str(tbl_prefix))

try:
    db_host = os.environ['DBHOST']
    db_user = os.environ['DBUSERNAME']
    db_pass = os.environ['DBPASSWORD']
    db_name = os.environ['DBNAME']
except:
    print("This script requires DBHOST, DBUSERNAME, DBPASSWORD, and DBNAME environment variables.")  
    sys.exit(1)

print("Creating tables...\n")

db = pymysql.connect(host=db_host, port=3306, user=db_user, passwd=db_pass, db=db_name)

try:
    cursor = db.cursor()
except Exception as e:
    print("\n{}\n".format(e))
    db.close()
    sys.exit(1)

tables = ['sql_tblogingest', 'sql_tblogcut', 'sql_tbrequestdigest', 'sql_tbcutbreakout']

for table in tables:
    create_query = getattr(queries,table)(tbl_prefix)
    try:
        print("Executing query: {}".format(create_query))
        cursor.execute(create_query)
        print()
    except Exception as e:
        print("\n{}\n".format(e))
        db.close()
        sys.exit(1)

db.close()
print("If you made it here,\n\tall of your tables were created.")

