# !/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: Li JIANG
"""
BEGIN
function:
    mysql connector and operator
END
"""

import pymysql
import init_env as env


def _update_sql(db, sql=''):
    sql = sql.strip(' ')
    if sql == '':
        return None
    elif sql[-1] != ';':
        sql += ';'
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()


def _update_many_sql(db, sql_many, field=None):
    if not field:
        raise RuntimeError('field not given!')
    sql = f'update news set {field}=%s where {field}=%s;'
    cursor = db.cursor()
    cursor.executemany(sql, sql_many)
    db.commit()


def update_pubtime(db):
    sql = 'select pubtime from news where pubtime REGEXP "^../";'
    cursor = db.cursor()
    cursor.execute(sql)
    wrong_pubtime = cursor.fetchall()
    print(len(wrong_pubtime))
    sql_many = []
    for ind, val in enumerate(wrong_pubtime):
        val = val[0]
        val_new = val[6:10]+'-'+val[0:2]+'-'+val[3:5]+val[10:]
        # ##sql = 'update news set pubtime=replace(pubtime, "{}", "{}") where pubtime REGEXP "^../";'.format(val, val[6:10]+'-'+val[0:2]+'-'+val[3:5]+val[10:])
        # sql = f'update news set pubtime={val_new} where pubtime={val};'
        # _update_sql(op, sql)
        sql_many.append((val_new, val))
        print(ind)
        # break
    _update_many_sql(db, sql_many, 'pubtime')
    cursor = db.cursor()
    cursor.execute(sql)
    wrong_pubtime = cursor.fetchall()
    print(len(wrong_pubtime))


def update_idhash(db):
    sql = 'select idhash, title, country from news where provider="jeuneafrique";'
    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    print(len(data))
    sql_many = []
    for ind, val in enumerate(data):
        idhash_old = val[0]
        title = val[1]
        country = val[2]
        idhash_new = env.generate_id(title+country)
        sql_many.append((idhash_new, idhash_old))
        # print(ind, idhash_old, idhash_new, title, country)
        # break
    _update_many_sql(db, sql_many, 'idhash')


if __name__ == '__main__':
    db = pymysql.connect(host=env.HOST_DB, user=env.USER_DB, password=env.PWD_DB, database=env.NAME_DB, port=env.PORT_DB,
                         charset="utf8mb4")  ##mysql 连接

    # update_pubtime(db)
    update_idhash(db)
    db.close()
