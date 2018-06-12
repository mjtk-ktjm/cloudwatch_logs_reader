import pymysql
import os, sys
import re
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

input_path = ''
process_log_stream = ''
files_to_parse = []

if len(input_path)==0:
    print("Please specify a logfiles source directory to run this script.")
    sys.exit(1)

search_regex = re.compile('\.gz$')

dir_list = os.scandir(input_path)
for dir_file in dir_list:
    if dir_file.is_dir():
        entry_full_path = os.path.join(input_path, dir_file.name)
        dir_list_sub = os.scandir(entry_full_path)
        for dir_file_sub in dir_list_sub:
            result = search_regex.search(dir_file_sub.name)
            if result is None:
                files_to_parse.append(os.path.join(entry_full_path, dir_file_sub.name))

#  TODO Here we should apply a signature to each line of log
#  This will help to identify uncaught conditions in the parse input so that they can all be eventually handled
#  Can also output whether log contains detailed logging details etc

#  charset is necessary for multilanguage support here
db = pymysql.connect(host=db_host, 
    port=3306, 
    user=db_user, 
    passwd=db_pass, 
    db=db_name, 
    charset='utf8')

try:
    cursor = db.cursor()
    # more charset support, not sure if it is necessary
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')
except Exception as e:
    print("\n{}\n".format(e))
    db.close()
    print('Failed to establish db cursor."')
    sys.exit()

# keep track of file and line number processed to ensure smooth gap handling
for file_to_parse in files_to_parse:
    print("Processing file:\t{}".format(file_to_parse))    
    with open(file_to_parse, 'r') as file_text:
        for line in file_text:
            line_parts = line.split(' ')
            if len(line_parts) < 3:
                print("ERROR: Not enough line parts.")
                continue
            line_datetime = line_parts.pop(0) 
            line_request_id = line_parts.pop(0)[1:-1] 
            line_body = str.strip(' '.join(line_parts))
 
            # basic line item insertion for rds
            try:
                sql = etl_loader(tbl_prefix, [process_log_stream, line_request_id, line_datetime, pymysql.escape_string(line_body)])
                # maybe a better way to batch these?
                cursor.execute(sql)
            except Exception as e:
                print("\n{}\n".format(e))
                db.close()
                print('Failed on insert:\n{}\n\t{}'.format(file_to_parse, line))
                sys.exit()
    # lots of logs, so go ahead and incremental insert (once per input logfile)
    db.commit()
db.close()

print('This is the END')

