import pandas as pd
import commons, os
import time
import logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import multiprocessing as multi, traceback
from dao import PostgreSQLCon as db
from psycopg2.extras import execute_batch

URL = commons.get_prop('common', 'ipo-url')
PROCESS_COUNT = commons.get_prop('common', 'process_cnt')
logger.init("IPOMC Reader", c.INFO)
log = logging.getLogger("IPOMC Reader")

DATABASE_COLUMNS = ['NAME', 'LISTED_DATE', 'ISSUED_PRICE', 'LISTED_PRICE', 'LISTED_GAIN',
                                     'CURR_PRICE', 'PROFIT_LOSS', 'NSE_CODE', 'OPEN_PRICE', 'HIGH_PRICE',
                                     'LOW_PRICE', 'VOLUME', 'PREV_PRICE']


def create_update_query(table):
    """This function creates an upsert query which replaces existing data based on primary key conflicts"""
    columns = ', '.join(DATABASE_COLUMNS)
    constraint = 'NSE_CODE'
    placeholder = "%s, to_date(%s, 'MON-DD-YYYY'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
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
    df = pd.DataFrame(data, columns=['NAME', 'LISTED_DATE', 'ISSUED_PRICE', 'LISTED_PRICE', 'LISTED_GAIN',
                                     'CURR_PRICE', 'PROFIT_LOSS', 'NSE_CODE', 'OPEN_PRICE', 'HIGH_PRICE',
                                     'LOW_PRICE', 'VOLUME', 'PREV_PRICE'])
    # Replace special char from below columns
    cols = ['LISTED_GAIN', 'PROFIT_LOSS']
    df[cols] = df[cols].replace({'%': ''}, regex=True)
    # Drop a row by condition
    df = df[df['LISTED_DATE'].notnull()]
    # convert numeric except listed columns
    drop_cols = ['LISTED_DATE', 'NSE_CODE', 'NAME']
    cols = df.columns.drop(drop_cols)
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    df = df.fillna(0)
    return df


def get_listed_ipo(stock_url):
    jobs = []
    spipe_list = []
    # Variables declaration
    start = time.time()
    # Get the shares from chitt
    urls = get_list_of_urls(stock_url)
    # urls = ['http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp', 'http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp?FormIPOPT_Page=2', 'http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp?FormIPOPT_Page=3', 'http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp?FormIPOPT_Page=4']
    print(urls)
    cpdus = multi.cpu_count()
    page_bins = h.chunks(cpdus, urls)
    for cpdu in range(cpdus):
        recv_end, send_end = multi.Pipe(False)
        worker = multi.Process(target=process_page, args=(page_bins[cpdu], send_end))
        worker.daemon = True
        jobs.append(worker)
        spipe_list.append(recv_end)
        worker.start()

    for job in jobs:
        job.join(timeout=10)
    print("All jobs completed......")
    try:
        ipo_size = 0
        result_list = [x.recv() for x in spipe_list]
        curr, con = db.get_connection()
        statement = create_update_query('IPO_STK_DETAILS')
        for results in result_list:
            for data in results:
                values = [data[k] for k in data]
                ipo_size += len(values)
                df = get_data_frame(values)
                records = df.to_dict(orient='records')
                print(records)
                execute_batch(curr, statement, df.values)
                con.commit()
        db.close_connection(con, curr)
        print("IPOs listed so far = {}".format(ipo_size))
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
            ipo_d = get_ipo_details_perf(data)
            results.append(ipo_d)
        except Exception as err:
            print("Failed exception = ", str(err))
    # After all data completed then send
    print("Sending results {} to Main process from {}".format(len(results), multi.current_process().name))
    # print("Error results {} to Main process from {}".format(failed_que.qsize(), multi.current_process().name))
    send_end.send(results)


def get_nse_code(url):
    nse_code, isin_code = None, None
    try:
        bs = h.parse_url(url)
        if bs:
            divs = bs.findAll("div", {'class': "panel panel-default"})[2].find('div', {'class': 'panel-body'})
            sub_div = divs.descendants
            for div in sub_div:
                if div.name == "li":
                    value = div.text
                    if "NSE Symbol:" in value:
                        nse_code = value.split(":")[1].strip()
                    elif "ISIN:" in value:
                        isin_code = value.split(":")[1].strip()
            if not nse_code:
                nse_code = isin_code

    except Exception as err:
        print("While parsing for NSE code", str(err))
    return nse_code


def get_ipo_day_info(link):
    ipo_details = []
    try:
        bs = h.parse_url(link)
        print("URL DAY=", link)
        if bs:
            divs = bs.find("div", {'class': 'col-lg-12 col-md-12 col-sm-12 main'}).findAll('div')
            nse = divs[2].text.split(":")[1].strip()
            face_val = divs[3].text.split(":")[1].strip()
            isin = divs[6].text.split(":")[1].strip()
            if nse and len(nse) > 0:
                ipo_details.append(nse)
            else:
                ipo_details.append(isin)
            ipo_details.append(face_val)
            table = bs.find("table", {"class": "table table-condensed table-bordered table-striped table-nonfluid"})
            rows = table.findAll("tr")
            # curr_price = rows[1].findAll("td")[0].find('span').text.strip()
            open_price = rows[2].findAll("td")[1].text.strip()
            hl = rows[3].findAll("td")[1].text.split("-")
            high, low = hl[0].strip(), hl[1].strip()
            prev_price = rows[4].findAll("td")[1].text.strip()
            turn_over = rows[8].findAll("td")[1].text.strip()
            # ipo_details.append(curr_price)
            ipo_details.append(open_price)
            ipo_details.append(high)
            ipo_details.append(low)
            ipo_details.append(turn_over)
            ipo_details.append(prev_price)
            if len(ipo_details) == 8:
                return ipo_details
    except Exception as err:
        traceback.print_exc()
        print("Exception =", str(err))
    return ipo_details


def get_listed_details(element):
    day_trade = []
    # Get data from element
    data = element.text.strip().split('\n')
    for record in data:
        if ": " in record:
            if not (("Change(Rs):" in record) or ('Last Trade(Rs):' in record)):
                day_trade.append(record.split(":")[1].strip())
    url = None
    for a in element.find_all('a', href=True):
        if "//www.chittorgarh.com/ipo/" in a['href']:
            url = a['href']
    print("FINAL URL = ", url)
    if url:
        nse_code = get_nse_code(url)
        day_trade.insert(0, nse_code)
    # print("TRADE = ", day_trade)
    return day_trade


def get_ipo_details_perf(url):

    ipo_details = {}
    try:
        bs = h.parse_url(url)
        print("URL = ", url)
        if bs:
            table = bs.find("table", {"class": "table table-bordered table-condensed"})
            rows = table.findAll("tr")
            row = 1
            while row < len(rows):
                if row%2 == 1:
                    cols = rows[row].find_all('td')
                    i = 0
                    ele_list = []
                    __key = None
                    for ele in cols:
                        i += 1
                        if i == 1: __key = ele.text.strip()
                        __val = ele.text.strip()
                        if i == 4:
                            # date massage
                            day_list = __val.split(" ")
                            if len(day_list) == 3:
                                if len(day_list[1].strip(",")) == 1:
                                    day_list[1] = '0' + day_list[1].strip(",")
                                else:
                                    day_list[1] = day_list[1].strip(",")
                                __val = "-".join(day_list)
                            else:
                                __val = "-".join(['Jan', '01', '1900'])
                        if not (i == 2 or i == 3):
                            ele_list.append(__val)
                    ipo_details[__key] = ele_list

                elif row%2 == 0:
                    divs = rows[row].find_all('div')[0]
                    sub_div = divs.descendants
                    i = 0
                    for div in sub_div:
                        if i == 5:
                            day_data = get_listed_details(div)
                            h.upd_dic_with_sub_list_ext(__key, day_data, ipo_details)
                        i += 1

                row += 1
    except Exception as err:
        traceback.print_exc()
        print("Exception =", str(err))
    return ipo_details


def get_list_of_urls(url):
    form = "?FormIPOPT_Page="
    link_arr = []
    link_arr.append(url)
    i = 2
    while i <= 21:
        link = url + form + str(i)
        link_arr.append(link)
        i += 1
    return link_arr


if __name__ == "__main__":
    # create_update_query('IPO_STK_DETAILS')
    get_listed_ipo(URL)
    # get_ipo_details_perf('http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp?FormIPOPT_Page=3')
    # get_nse_code('http://www.chittorgarh.com/ipo/cochin-shipyard-ipo/684/')
