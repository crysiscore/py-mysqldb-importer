import pymysql  # uses a c extension to connect to mysql
import MySQLdb  # pure python library
import os
import logging
import sys
import yaml
import subprocess
import re

# Read config file
with open("/home/agnaldo/config.yaml", 'r') as stream:
    try:
        config = yaml.load(stream)

    except yaml.YAMLError as exc:
        logging.debug(str(exc))
    else:
        mysql_username = config['mysql_username']
        mysql_password = config['mysql_password']
        mysqlhost = config['host']

# Misc Functions
def mysqlgetconnection(host, usr, pwd):
    # Connect to the database
    connection = pymysql.connect(host=host,
                                 user=usr,
                                 password=pwd)

    return connection

def createdatabase(conn, dbname, pathtofile):
    counter = 0
    try:
        with conn.cursor() as cursor:
            # drop database if exists
            sql_dropdb = "drop database if exists " + dbname[:len(dbname) - 4].lower() + " ;"
            cursor.execute(sql_dropdb)
            conn.commit()
            # Create db
            sql_create = "create database " + dbname[:len(dbname) - 4].lower() + " ;"
            cursor.execute(sql_create)
            # connection is not autocommit by default. So you must commit to save
            # your changes.
            conn.commit()
            importdbfromfile(conn, dbname[:len(dbname) - 4].lower(), dbname, pathtofile)
            counter += 1
            # cursor.execute("use " + dbname[:len(dbname) - 4].lower() + " ;")
            # sql_import_db = "source '" + os.path.join(pathtofile, dbname) + "' ;"
            # cursor.execute(sql_import_db)

    except pymysql.err.ProgrammingError as err:
        logging.debug("Error executing sqlquery: " + sql_create + str(err.args))
        print("Error executing sqlquery: " + sql_create + str(err.args))

    return counter

def beginmysqljob(extracted_files, sql_files_dir):
    if os.path.isdir(sql_files_dir):
        os.chdir(sql_files_dir)
        try:
            con = mysqlgetconnection(host=mysqlhost, usr=mysql_username, pwd=mysql_password)
            if con is None:
                print("Cannot connect to mysql server,. check if server is running or you username & password")
            else:
                print("Connection to mysql established.")
                print("Starting mysql jobs...")
                nrdatabases = 0
                for root, dirs, files in os.walk('.', topdown=True):
                    for n in files:
                        if n.endswith(".sql"):
                            nrdatabases += createdatabase(con, n, sql_files_dir)
        except pymysql.OperationalError as op_err:
            if op_err.args[0] == 'PYMYSQL_DUPLICATE_ERROR':
                print(str(op_err.args[0]))
                logging.debug(str(op_err.args[0]))
        except pymysql.ProgrammingError as pro_err:
            if pro_err.args[0] == 'SYNTAX_ERROR':
                print(str(pro_err.args[0]))
                logging.debug(str(pro_err.args[0]))
        except:
            logging.debug("Unexpected error on beginmysql jobs task", sys.exc_info()[0])
            print("Unexpected error on beginmysql jobs task", sys.exc_info()[0])
        finally:
            con.close()

        if nrdatabases == 0:
            logging.info("No database created, cheack erros above")
            print("No database created, cheack erros above")

        if nrdatabases != extracted_files:
            logging.info("Some databases where not created. check the logs for more details")
            print("Some databases where not created. check the logs for more details")
        if nrdatabases == extracted_files:
            print('Process ended successfully')

            # pymysql error handling
            # _map_error(ProgrammingError, ER.DB_CREATE_EXISTS, ER.SYNTAX_ERROR,
            #        ER.PARSE_ERROR, ER.NO_SUCH_TABLE, ER.WRONG_DB_NAME,
            #        ER.WRONG_TABLE_NAME, ER.FIELD_SPECIFIED_TWICE,
            #        ER.INVALID_GROUP_FUNC_USE, ER.UNSUPPORTED_EXTENSION,
            #        ER.TABLE_MUST_HAVE_COLUMNS, ER.CANT_DO_THIS_DURING_AN_TRANSACTION)
            # _map_error(DataError, ER.WARN_DATA_TRUNCATED, ER.WARN_NULL_TO_NOTNULL,
            #        ER.WARN_DATA_OUT_OF_RANGE, ER.NO_DEFAULT, ER.PRIMARY_CANT_HAVE_NULL,
            #        ER.DATA_TOO_LONG, ER.DATETIME_FUNCTION_OVERFLOW)
            # _map_error(IntegrityError, ER.DUP_ENTRY, ER.NO_REFERENCED_ROW,
            #        ER.NO_REFERENCED_ROW_2, ER.ROW_IS_REFERENCED, ER.ROW_IS_REFERENCED_2,
            #        ER.CANNOT_ADD_FOREIGN, ER.BAD_NULL_ERROR)
            # _map_error(NotSupportedError, ER.WARNING_NOT_COMPLETE_ROLLBACK,
            #        ER.NOT_SUPPORTED_YET, ER.FEATURE_DISABLED, ER.UNKNOWN_STORAGE_ENGINE)
            # _map_error(OperationalError, ER.DBACCESS_DENIED_ERROR, ER.ACCESS_DENIED_ERROR,
            #        ER.CON_COUNT_ERROR, ER.TABLEACCESS_DENIED_ERROR,
            #        ER.COLUMNACCESS_DENIED_ERROR)

def importdbfromfile(con, dbname, filename, sql_datadir):
    # command_line = "mysql -uroot -ph4l0h4l0 -Dteste < '/home/agnaldo/CCS/OpenMRS/sql_data_files/teste.sql'"
    if isdatabase(con, dbname):
        command_line = "mysql -u" + mysql_username + " -p" + mysql_password + " -D" + dbname + " < " + "'" + sql_datadir + '/' + filename + "'"
        try:
            print('Creating db: ' + dbname.lower())
            return_code = subprocess.check_call(command_line, bufsize=-1, shell=True)
            print('Database ' + dbname + ' imported successfully...')
        except subprocess.CalledProcessError as err:
            logging.debug('Database not imported, ' + str(err.__str__()))
            print('Error: ' + str(err.__str__()))
    else:
        print(dbname + ' not created')
        # returncode = 1  # 1 is default for error (non 0 exit-code)
    return return_code

def isdatabase(con, dbname):
    sql_query = "show databases;"
    found = False
    try:
        with con.cursor() as cursor:
            cursor.execute(sql_query)
            listd = []
            for row in cursor:
                listd.append(str(row))
            for st in listd:
                if re.search(dbname, st):
                    found = True
    except pymysql.err.ProgrammingError as err:
        print(str(err.args))
    return found
