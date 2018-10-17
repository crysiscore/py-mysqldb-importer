import subprocess
import yaml
import pymysql
import logging
import re

command_line = "mysql -uroot -ph4l0h4l0 -Dteste < '/home/agnaldo/CCS/OpenMRS/sql_data_files/teste.sql'"


# args = shlex.split(command_line)
# print(args)
def isdatabase(con, dbname):
    sql_query = "show databases;"
    try:
        with con.cursor() as cursor:
            cursor.execute(sql_query)
            listd = []
            for row in cursor:
                listd.append(str(row))
            for st in listd:
                if re.search(dbname, st):
                    return True
    except pymysql.err.ProgrammingError as err:
        print(str(err.args))


def importdbfromfile(con, dbname, filename, sql_datadir):
    # command_line = "mysql -uroot -ph4l0h4l0 -Dteste < '/home/agnaldo/CCS/OpenMRS/sql_data_files/teste.sql'"
    if isdatabase(con, dbname):
        command_line = "mysql -u" + mysql_username + " -p" + mysql_password + " -D" + dbname + " < " + "'" + sql_datadir + filename + "'"
        print(command_line)


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


try:
    # a = subprocess.check_call(command_line, bufsize=-1, shell=True)
    # print(a)
    con = mysqlgetconnection(mysqlhost, mysql_username, mysql_password)
    importdbfromfile(con, 'teste', 'teste.sql', '/home/agnaldo/CCS/OpenMRS/sql_data_files')

except subprocess.CalledProcessError as err:
    print('Error: ' + str(err.__str__()))
    print('Error: ' + str(err.returncode))

# out outputoutput= p.communioutput cate()[0]
# print('\t combined output:', str(p))
# print('\tstderr value   :', repr(stderr_value))
