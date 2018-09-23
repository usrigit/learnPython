import logging
from commons import Constants as c
from commons import logger
import multiprocessing as multi
from pandas import Series
import pandas as pd
from dao import PostgreSQLCon as db
import commons.Helper as h
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_batch
import traceback, time

NO_URL_COUNT = 1500
YAHOO_URL = "https://in.finance.yahoo.com"
logger.init("YAHOO_STK Perf Reader", c.INFO)
log = logging.getLogger("YAHOO_STK Perf Reader")


def parse_yahoo_stk_hist(url):
    try:
        name = url.split("=")[1].split(".")[0]
        # print("Name = ", name)
        bs = h.parse_url(url)
        if bs:
            table = bs.find('div', {'class': "Pb(10px) Ovx(a) W(100%)"}).find_all("table", {"class": "W(100%) M(0)"})[0]
            data = [
                       [td.string.strip() for td in tr.find_all('td') if td.string]
                       for tr in table.find_all('tr')[2:]
                   ][:-1]
            # print(data)
            # data.insert(0, ['STK_DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'ACLOSE', 'VOLUME'])
            df = get_data_frame(data, name)
            if len(df) > 0:
                df_columns = list(df)
                table = "STK_INFO_HISTORY"
                constraint = ', '.join(['STK_DATE', 'NSE_CODE'])
                values = "to_date(%s, 'DD-MON-YYYY'), %s, %s, %s, %s, %s, %s"
                insert_stmt = h.create_update_query(table, df_columns, values, constraint)
                conn = psycopg2.connect(database="trading",
                                        user="postgres",
                                        password="postgres")
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = conn.cursor()
                execute_batch(cursor, insert_stmt, df.values)
                conn.commit()
                db.close_connection(conn, cursor)
    except Exception as err:
        traceback.print_exc()
        print("Exception = ", str(err))


def get_data_frame(data, name):
    # Set pandas options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', 0)
    df = pd.DataFrame(data, columns=['STK_DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'ACLOSE', 'VOLUME'])
    df = df.assign(NSE_CODE=Series(name, index=df.index))
    df = df.drop(columns='ACLOSE')
    cols = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
    df[cols] = df[cols].replace({'\$': '', ',': ''}, regex=True)
    # Drop a row by condition
    df = df[df['OPEN'].notnull()]
    drop_cols = ['STK_DATE', 'NSE_CODE']
    cols = df.columns.drop(drop_cols)
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    df = df.fillna(0)
    return df


def get_stk_history():
    jobs = []
    # Variables declaration
    start = time.time()
    # Get the shares from yahoo
    urls = get_yahoo_fin_urls()
    # urls = ["https://in.finance.yahoo.com/quote/3IINFOTECH.NS/history?p=3IINFOTECH.NS", "https://in.finance.yahoo.com/quote/3IINFOTECH.NS/history?p=3IINFOTECH.NS", "https://in.finance.yahoo.com/quote/3IINFOTECH.NS/history?p=3IINFOTECH.NS", "https://in.finance.yahoo.com/quote/3IINFOTECH.NS/history?p=3IINFOTECH.NS"]
    print("No of URLs = ", len(urls))
    cpdus = multi.cpu_count()
    page_bins = h.chunks(cpdus, urls)
    for chunk in page_bins:
        worker = multi.Process(target=process_page, args=(chunk,))
        worker.daemon = True
        jobs.append(worker)
        worker.start()

    for job in jobs:
        job.join(timeout=10)
    print("All jobs completed......")
    print("Execution time = {0:.5f}".format(time.time() - start))


def process_page(data_array):
    print("Size of the data array from current process {} is {} "
          .format(multi.current_process().name, len(data_array)))
    for url in data_array:
        try:
            print("Parsing URL = ", url)
            parse_yahoo_stk_hist(url)
        except Exception as err:
            print("Failed exception = ", str(err))
    print("Completed Main process from {}".format(multi.current_process().name))


def get_yahoo_fin_urls():
    i = 0
    url_list = []
    while i < NO_URL_COUNT:
        url = YAHOO_URL + "/most-active?offset=" + str(i) + "&count=25"
        url_list.append(url)
        i += 25
    urls = []
    for url in url_list:
        try:
            bs = h.parse_url(url)
            if bs:
                std_data = bs.find('div', {'class': "Ovx(s)"}).find_all("table", {"class": "W(100%)"})[0].find_all('tr')
                for tr in std_data:
                    link = tr.find('a', href=True, text=True)['href']
                    url = (YAHOO_URL + link).replace('?p', '/history?p')
                    if url not in urls:
                        urls.append(url)
        except Exception as err:
            print("Exception = ", str(err))

    return urls


if __name__ == "__main__":
    get_stk_history()
