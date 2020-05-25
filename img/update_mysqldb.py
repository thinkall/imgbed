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
import argparse
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


def update_idhash(db, start=0, lines=20):
    sql = f'select idhash, title, country from news order by pubtime desc limit {start}, {lines};'
    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    print(len(data))

    n, sql_many = 0, []
    for ind, val in enumerate(data):
        idhash_old = val[0]
        title = val[1]
        country = val[2]
        idhash_new = env.generate_id(title+country)
        sql_many.append((idhash_new, idhash_old))
        if lines - start <= 20:
            print(ind, idhash_old, idhash_new, title, country)
        # break
        n = n + 1
        if n > 1000:
            _update_many_sql(db, sql_many, 'idhash')
            print(f'{ind}, committed part of the task.')
            env.logger.info(f'{ind}, committed part of the task.')
            n, sql_many = 0, []
    if n > 0:
        _update_many_sql(db, sql_many, 'idhash')
    print(f'{ind}, committed all of the task.')
    env.logger.info(f'{ind}, committed all of the task.')


if __name__ == '__main__':
    db = pymysql.connect(host=env.HOST_DB, user=env.USER_DB, password=env.PWD_DB, database=env.NAME_DB, port=env.PORT_DB,
                         charset="utf8mb4")  ##mysql 连接

    # update_pubtime(db)

    parser = argparse.ArgumentParser(
        description='update idhash from start row to end row')
    parser.add_argument('--start', type=int, default=1400)
    parser.add_argument('--lines', type=int, default=1700)
    args = parser.parse_args()
    update_idhash(db, args.start, args.lines)
    db.close()
