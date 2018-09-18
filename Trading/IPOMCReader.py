import pandas as pd
import commons, os
import time
import logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import multiprocessing as multi, traceback
import matplotlib.pyplot as plt

URL = commons.get_prop('common', 'ipo-url')
PROCESS_COUNT = commons.get_prop('common', 'process_cnt')
logger.init("IPOMC Reader", c.INFO)
log = logging.getLogger("IPOMC Reader")


def get_listed_ipo(stock_url):
    jobs = []
    spipe_list = []
    # Variables declaration
    start = time.time()
    # Get the shares from chitt
    # urls = get_list_of_urls(stock_url)
    urls = ['http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp', 'http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp?FormIPOPT_Page=2', 'http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp?FormIPOPT_Page=3', 'http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp?FormIPOPT_Page=4']
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
        result_list = [x.recv() for x in spipe_list]
        for results in result_list:
            print("results = ", results)
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
            ipo_d, ipo_p = get_ipo_details_perf(data)
            for ipo in ipo_p:
                link = ipo_p.get(ipo)
                ipo_details = get_ipo_day_info(link)
                if ipo_details:
                    h.upd_dic_with_sub_list_ext(ipo, ipo_details, ipo_d)
            results.append(ipo_d)
        except Exception as err:
            print("Failed exception = ", str(err))
    # After all data completed then send
    print("Sending results {} to Main process from {}".format(len(results), multi.current_process().name))
    # print("Error results {} to Main process from {}".format(failed_que.qsize(), multi.current_process().name))
    send_end.send(results)


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
            curr_price = rows[1].findAll("td")[0].find('span').text.strip()
            open_price = rows[2].findAll("td")[1].text.strip()
            hl = rows[3].findAll("td")[1].text.split("-")
            high, low = hl[0].strip(), hl[1].strip()
            prev_price = rows[4].findAll("td")[1].text.strip()
            turn_over = rows[8].findAll("td")[1].text.strip()
            ipo_details.append(curr_price)
            ipo_details.append(open_price)
            ipo_details.append(high)
            ipo_details.append(low)
            ipo_details.append(prev_price)
            ipo_details.append(turn_over)
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
            if not "Change(Rs):" in record:
                day_trade.append(record.split(":")[1].strip())
    print("day_trade = ", day_trade)
    url = None
    for a in element.find_all('a', href=True):
        if "http://www.chittorgarh.com/ipo/" in a['href']:
            url = a['href']

    return day_trade, url
def get_ipo_details_perf(url):

    ipo_details = {}
    ipo_code_day = {}
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
                        if not (i == 2 or i == 3):
                            ele_list.append(__val)
                    ipo_details[__key] = ele_list

                elif row%2 == 0:
                    divs = rows[row].find_all('div')[0]
                    sub_div = divs.descendants
                    i = 0
                    for div in sub_div:
                        if i == 5:
                            day_data, url = get_listed_details(div)
                            h.upd_dic_with_sub_list_ext(__key, day_data, ipo_details)
                        i += 1
                print("IOP details = ", ipo_details)
                row += 1
    except Exception as err:
        traceback.print_exc()
        print("Exception =", str(err))
    return ipo_details, ipo_code_day


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
    # get_listed_ipo(URL)
    get_ipo_details_perf('http://www.chittorgarh.com/ipo/ipo_perf_tracker.asp?FormIPOPT_Page=3')
