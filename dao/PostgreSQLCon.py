import psycopg2, commons
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_connection():
    conn = None
    cursor = None
    try:
        user_name = commons.get_prop('postgres', 'user-name')
        password = commons.get_prop('postgres', 'password')
        db_name = commons.get_prop('postgres', 'database')
        conn = psycopg2.connect(database=db_name,
                                user=user_name,
                                password=password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
    except psycopg2.DatabaseError as e:
        print("Error while connecting database: %s", e)
    return cursor, conn


def fetch_data(cursor, sql):
    try:
        cursor.execute(sql)
        fetch = cursor.fetchall()
        return fetch
    except Exception as err:
        print("while fetching data from table: %s", err)


query = "INSERT INTO STK_PERF_HISTORY (NSE_CODE, EPS, BOOK_VAL, DIVIDEND, RET_ON_EQ, " \
        "PRC_TO_BOOK, PRC_TO_SALE, CURR_RATIO, DEBT_EQUITY, STK_YEAR) " \
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"


def load_data(conn, cursor, sql, parameters):
    try:
        cursor.execute(sql, parameters)
        conn.commit()
    except Exception as err:
        print("while loading data to table: %s", err)


def close_connection(conn, cursor):
    try:
        cursor.close()
        if conn is not None:
            conn.close()
    except Exception as err:
        print("Error while closing the connection: %s", err)