#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


"""
performance test to insert contents.
"""
import cx_Oracle


from idds.common.config import config_get
# from idds.core.contents import add_content


def get_subfinished_requests(db_pool):
    connection = db_pool.acquire()

    req_ids = []
    sql = """select request_id from atlas_IDDS.requests where status in (4,5) and scope!='hpo'"""
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        # print(row)
        req_ids.append(row[0])
    cursor.close()

    connection.commit()
    db_pool.release(connection)
    print(req_ids)


def get_session_pool():
    sql_connection = config_get('database', 'default')
    sql_connection = sql_connection.replace("oracle://", "")
    user_pass, tns = sql_connection.split('@')
    user, passwd = user_pass.split(':')
    db_pool = cx_Oracle.SessionPool(user, passwd, tns, min=12, max=20, increment=1)
    return db_pool


def test():
    pool = get_session_pool()
    get_subfinished_requests(pool)


if __name__ == '__main__':
    test()
