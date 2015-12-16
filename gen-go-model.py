#coding:utf8
'''
使用 desc table; 解释MySQL的表结构。生成 golang的结构体定义
用法：

$> python gen-go-model.py  -D data_name -t table_name
$> 输出go结构体定义语法

'''
import sys
import logging
import time
import getopt
import _mysql

def now():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 

def escape(var):
    '''这里连接数据库都是使用utf8的。'''
    if var is None:
        return ''
    if isinstance(var, unicode):
        var = var.encode('utf8')
    if not isinstance(var, str):
        var = str(var)
    return _mysql.escape_string(var)

def dbOpen(host, port, user, password, dbname):
    conn = _mysql.connect(
            db=dbname,
            host=host,
            user=user,
            passwd=password,
            port=port)
    conn.query("set names utf8;")
    return conn

ROW_OF_IDX=0
ROW_OF_KEY=1
DEBUG=0
def query(db, sql, dataType=ROW_OF_KEY):
    global DEBUG
    if DEBUG and not sql.startswith('SELECT'):
        print 'in debug,just print:', sql
        return

    rows = ()
    try:
        db.query(sql)
        res = db.store_result()
        if res:
            rows = res.fetch_row(res.num_rows(), dataType)
    except Exception, e:
        print "[%s]\t[%s]" % (e, sql)
        raise e
    return rows

def execute(db, sql):
    if DEBUG:
        logging.warning("Debuging, just print SQL:%s", sql)
        return -110
    try:
        db.query(sql)
        res = db.affected_rows()
        if res < 0 or res == 0xFFFFFFFFFFFFFFFF:
            # ps : 0xFFFFFFFFFFFFFFFF (64位的-1) 
            # 这个值与驱动、系统、硬件CPU位数都可能有关
            logging.error('MySQL execute error n=[%d], sql=%s', res, sql)
        return res
    except Exception, e:
        logging.error("err=[%s]\tsql=[%s]", e, sql)
        if e[0] == 1062:
             return 0
        raise e
    return -120


def convertType(typ):
    typ=typ.lower()
    if typ.find('int')>0:
        return "int64"
    elif typ.find('char')>0 or typ.find("text") or typ.find("enum"):
        return "string"
    elif typ.find("decimal"):
        return "float64"
    elif typ.find('datetime'):
        return "time.Time"
    elif typ.find("bool"):
        return "bool"
    else:
        return typ

def gen(host,port,user,password,dbname,table):
    db = dbOpen(host, port, user, password, dbname)

    desc = query(db, "desc %s;" % escape(table), ROW_OF_KEY)

    code = ["type %s struct{" % table.title()]

    for row in desc:
        field = row['Field']
        typ = convertType(row['Type'])

        define = '    %s %s `db:"%s"    json"%s"` // %s' %(field.title(), typ, field,field, row['Type'])
        code.append(define)

    code.append("}")
    print '\n'.join(code)

def main():
    def usage():
        print "--help: print this message"

        print "-h --host, MySQL host"
        print "-P --port, MySQL port"
        print "-u --user, MySQL user"
        print "-p --password, MySQL password"
        print "-D --database, MySQL Database"
        print "-t --table name"
    try:
        opts, args = getopt.getopt(sys.argv[1:], "Hh:P:u:p:D:t:", 
            ["--help","redis=","host=","port=","user=","password=",
            "database=",'table='])
    except getopt.GetoptError:
        print usage()
        return 
    host='localhost'
    user="root"
    port=3306
    password=''
    dbname="dbname"
    table="tbname"

    for o, a in opts:
        if o in ("-H","--help"):
            usage()
            sys.exit()
        elif o in ("-h","--host"):
            host=a
        elif o in ("-P","--port"):
            port=int(a)
        elif o in ("-u","--user"):
            user=a
        elif o in("-p","--password"):
            password=a
        elif o in ("-D","--database"):
            dbname=a
        elif o in ('-t', '--table'):
            table=a

    logging.info("mysql=[%s:%s@%s:%s/%s?table=%s]",
        user, password, host, port, dbname, table)
    gen(host,port,user,password,dbname,table)
if __name__ == '__main__':
    main()
