import pandas as pd
import commons, pickle
import time
import datetime
import logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import multiprocessing as multi, traceback
from dao import PostgreSQLCon as db
from psycopg2.extras import execute_batch

PROCESS_COUNT = commons.get_prop('common', 'process_cnt')
logger.init("BONUSMC Reader", c.INFO)
log = logging.getLogger("BONUSMC Reader")

STK_DT_FORMAT = '%d-%m-%Y'
DATABASE_COLUMNS = ['NAME', 'STK_CODE', 'SECTOR', 'BONUS_COUNT', 'LST_BONUS_DATE', 'SPLIT_TIMES', 'LST_SPLIT_DATE',
                    'DIV_COUNT', 'LST_DIV_DATE']


def convert_strTDate(date_string):

    try:
        date_obj = datetime.datetime.strptime(date_string, STK_DT_FORMAT)
        return date_obj.date()
    except ValueError:
        # print("Incorrect data format, should be YYYY-MM-DD")
        return None


def create_update_query(table):
    """This function creates an upsert query which replaces existing data based on primary key conflicts"""
    columns = ', '.join(DATABASE_COLUMNS)
    constraint = 'STK_CODE'
    placeholder = "%s, %s, %s, %s, to_date(%s, 'DD-MM-YYYY'), %s, to_date(%s, 'DD-MM-YYYY'), %s, to_date(%s, 'DD-MM-YYYY')"
    updates = ', '.join([f'{col} = EXCLUDED.{col}' for col in DATABASE_COLUMNS])
    query = f"""INSERT INTO {table} ({columns}) 
                VALUES ({placeholder}) 
                ON CONFLICT ({constraint}) 
                DO UPDATE SET {updates};"""
    query.split()
    query = ' '.join(query.split())
    print(query)
    return query


def get_data_frame(data):
    # Set pandas options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', 0)
    df = pd.DataFrame(data, columns=['NAME', 'STK_CODE', 'SECTOR', 'BONUS_COUNT', 'LST_BONUS_DATE',
                                     'SPLIT_TIMES', 'LST_SPLIT_DATE', 'DIV_COUNT', 'LST_DIV_DATE'])
    df = df.fillna(0)
    return df


def get_listed_links():
    jobs = []
    spipe_list = []
    # Variables declaration
    start = time.time()
    # Get the shares from chitt
    mc_bonus_file = 'D:/Projects/work/Trading/sql/mc_bonus_stocks.csv'
    with open(mc_bonus_file, 'rb') as fp:
        links = pickle.load(fp)
    cpdus = multi.cpu_count()
    page_bins = h.chunks(cpdus, links)
    for cpdu in range(cpdus):
        recv_end, send_end = multi.Pipe(False)
        worker = multi.Process(target=process_page, args=(page_bins[cpdu], send_end))
        worker.daemon = True
        jobs.append(worker)
        spipe_list.append(recv_end)
        worker.start()

    for job in jobs:
        job.join(timeout=100)
    print("All jobs completed......")
    try:
        ipo_size = 0
        result_list = [x.recv() for x in spipe_list]
        curr, con = db.get_connection()
        statement = create_update_query('STK_BONUS_INFO')
        # print("res = ", result_list)
        for results in result_list:
            if len(results) > 0:
                ipo_size += len(results)
                df = get_data_frame(results)
                log.info(df.values)
                execute_batch(curr, statement, df.values)
                con.commit()
        db.close_connection(con, curr)
        print("BONUS listed so far = {}".format(ipo_size))
    except Exception as err:
        traceback.print_exc()
        print(str(err))
    print("Execution time = {0:.5f}".format(time.time() - start))


def process_page(data_array, send_end):
    results = []
    print("Size of the data array from current process {} is {} "
    .format(multi.current_process().name, len(data_array)))
    for data in data_array:
        try:
            bonus_stk = get_bonus_split_details(data)
            if bonus_stk and len(bonus_stk) > 0:
                results.append(bonus_stk)
        except Exception as err:
            print("Failed exception = ", str(err))
    # After all data completed then send
    print("Sending results {} to Main process from {}".format(len(results), multi.current_process().name))
    send_end.send(results)


def get_bonus_split_details(cmp_url):

    try:
        # print("CMP URL = ", cmp_url)
        bs = h.parse_url(cmp_url)
        cmp_name = None
        title_data = bs.find('div', {'id': 'nChrtPrc'}).find('h1', {'class': 'b_42 PT20'})
        if title_data:
            cmp_name = title_data.text.strip()
        std_data = bs.find('div', {'class': 'PB10'}).find('div', {'class': 'FL gry10'})
        if std_data:
            header_parts = std_data.text.split("|")
            nse_code, isin, sector = None, None, None
            for part in header_parts:
                name, val = part.split(":")[0].strip(), part.split(":")[1].strip()
                if name == 'ISIN': isin = val
                if name == 'NSE': nse_code = val
                if name == 'SECTOR': sector = val
            if not nse_code: nse_code = isin
        data = [
            [
                [td.string.strip() for td in tr.find_all('td') if td.string]
                for tr in table.find_all('tr')[1:]
            ]
            for table in bs.find_all("table", {"class": "tbldivid"})
        ]

        bonus_count = len(data[0])
        if bonus_count > 1:
            last_bonus_dt = data[0][0][0]
            last_bonus_date = convert_strTDate(last_bonus_dt)
            if last_bonus_date is None:
                bonus_count = 0
                last_bonus_dt = '01-01-1900'
        else:
            bonus_count = 0
            last_bonus_dt = '01-01-1900'

        split_url = cmp_url.replace("bonus", "splits")
        bs = h.parse_url(split_url)
        data = [
            [
                [td.string.strip() for td in tr.find_all('td') if td.string]
                for tr in table.find_all('tr')[1:]
            ]
            for table in bs.find_all("table", {"class": "tbldivid"})
        ]

        split_times = len(data[0])
        if split_times > 1:
            last_split_dt = data[0][0][0]
            last_split_date = convert_strTDate(last_split_dt)
            if last_split_date is None:
                split_times = 0
                last_split_dt = '01-01-1900'
        else:
            split_times = 0
            last_split_dt = '01-01-1900'

        div_url = cmp_url.replace("bonus", "dividends")
        bs = h.parse_url(div_url)
        data = [
            [
                [td.string.strip() for td in tr.find_all('td') if td.string]
                for tr in table.find_all('tr')[1:]
            ]
            for table in bs.find_all("table", {"class": "tbldivid"})
        ]

        div_cnt = len(data[0])
        if div_cnt > 1:
            last_div_dt = data[0][0][0]
            last_div_date = convert_strTDate(last_div_dt)
            if last_div_date is None:
                div_cnt = 0
                last_div_dt = '01-01-1900'
        else:
            div_cnt = 0
            last_div_dt = '01-01-1900'

        if split_times == 0 and bonus_count == 0 and div_cnt == 0:
            return None
        else:
            cmp_details = {}
            cmp_details['NAME'] = cmp_name
            cmp_details['STK_CODE'] = nse_code
            cmp_details['SECTOR'] = sector
            cmp_details['BONUS_COUNT'] = bonus_count
            cmp_details['LST_BONUS_DATE'] = last_bonus_dt
            cmp_details['SPLIT_TIMES'] = split_times
            cmp_details['LST_SPLIT_DATE'] = last_split_dt
            cmp_details['DIV_COUNT'] = div_cnt
            cmp_details['LST_DIV_DATE'] = last_div_dt
            return cmp_details

    except Exception as err:
        print("In get bonus ERROR = {} and URL =  {}".format(str(err), cmp_url))
        traceback.print_exc()
        return {}


if __name__ == "__main__":
    get_listed_links()
    # print(convert_strTDate('10-12-2015'))
