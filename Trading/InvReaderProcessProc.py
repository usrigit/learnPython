from multiprocessing import Lock, Process, Queue, current_process
import multiprocessing as multi
import time
import queue  # imported for using queue.Empty exception
import logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import pandas as pd
import commons, os, numpy as np
import threading

logger.init("Investello Reader Process Proc", c.INFO)
log = logging.getLogger("Investello Reader Process Proc")

URL = "https://www.investello.com/Companies"


def chunks(n, page_list):
    """Splits the list into n chunks"""
    return np.array_split(page_list, n)


def get_list_of_share_links(stock_url):
    data_que = Queue()
    shares = get_shrs_from_investello(stock_url)
    for title, link in shares.items():
        base_link = link
        growth_link = base_link.replace('Dashboard', 'Growth')
        if title and link:
            data_que.put("###".join([title, link]))
            data_que.put("###".join([title, growth_link]))
        #
    return data_que


def get_shrs_from_investello(url):
    """
        This function will parse html and return data
    :param url: home page url
    :param shares: dictionary contains result
    :return: dictionary which contains key as company name and value is url
    """
    shares = {}
    try:

        bs = h.parse_url(url)
        if bs:
            div = bs.find("div", {"class": "stock-list container"})
            # https://www.investello.com/Companies
            for link in div.findAll("a"):
                shares[link.text] = "https://www.investello.com" + link.get('href')

    except Exception as err:
        log.exception("ERROR in get_shr_from_mnctl = ", str(err))
    return shares


def get_shares_details(stock_url, loc, pc_cnt):
    # Variables declaration
    jobs = []
    dashboard_que = Queue()
    growth_que = Queue()
    failed_que = Queue()
    start = time.time()
    # Get the shares from Investello
    data_que = get_list_of_share_links(stock_url)
    cpdus = 1
    print("Total Process count = {}".format(cpdus))
    print("Total Companies dashboard/Growth queue count = {}".format(data_que.qsize() / 2))

    for cpdu in range(cpdus):
        # recv_end, send_end = multi.Pipe(False)
        worker = Process(name=str(cpdu), target=process_page, args=(data_que, dashboard_que, growth_que, failed_que, loc))
        jobs.append(worker)
        worker.start()

    for proc in jobs:
        proc.join()
    print("All jobs completed.......")

    print("DASHBOARD process URL count = ", dashboard_que.qsize())
    print("GROWTH process URL count = ", growth_que.qsize())
    print("Failed process URL count = ", failed_que.qsize())

    # while not failed_que.empty():
    #     print("Failed URL = ", failed_que.get())

    success_list = []
    dashboard_dict = {}
    growth_dict = {}
    while not dashboard_que.empty():
        dashboard_dict.update(dashboard_que.get())

    while not growth_que.empty():
        growth_dict.update(growth_que.get())

    final_data = h.merge_dict_with_sub_dict(dashboard_dict, growth_dict)
    success_list = list(final_data.values())
    # Set pandas options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', 0)

    df = pd.DataFrame(success_list)
    df.set_index(keys=['NAME'], inplace=True)
    # print(df.describe().to_string())

    list_of_df = [g for _, g in df.groupby(['SECTOR', 'INDUSTRY'])]
    for sub_df in list_of_df:
        # print(sub_df)
        df_name = sub_df.iloc[0]['SECTOR'].upper() + "_" + sub_df.iloc[0]['INDUSTRY'].upper()
        sorted_df = sub_df.sort_values(by=['EPS', 'P/E'], ascending=[False, True])
        final_df = sorted_df.drop(['SECTOR', 'INDUSTRY'], axis=1)
        writer_orig = pd.ExcelWriter(os.path.join(commons.get_prop('base-path', 'output'), df_name + '_Listings.xlsx'),
                                     engine='xlsxwriter')
        final_df.to_excel(writer_orig, index=True, sheet_name='report')
        writer_orig.save()

    print("Execution time = {0:.5f}".format(time.time() - start))
    return True


def mny_ctr_shr_frm_url(cmp_name, cmp_url):
    comp_details = {}
    try:
        # print("URL = ", cmp_url)89

        comp_details['NAME'] = cmp_name
        bs = h.parse_url(cmp_url)
        if bs:
            header_data = bs.find('div', {'class': 'contentpanel'})
            # print(header_data)
            span_arr = header_data.findAll('span')
            sector = span_arr[2].text.strip()
            comp_details['SECTOR'] = sector
            industry = span_arr[3].text.strip()
            comp_details['INDUSTRY'] = industry
            last_close_price = span_arr[4].text[4:10].strip()
            comp_details['LCP'] = last_close_price
            for each_div in header_data.findAll('div', attrs={'class': 'panel-body people-info'}):
                sub_div = each_div.descendants
                for cd in sub_div:
                    if cd.name == 'div' and cd.get('class') == ['info-group']:
                        __tag_name = cd.label.text.strip()
                        __tag_val = cd.h4.text.strip()
                        if __tag_name == 'Book Value':
                            __tag_name = 'BOOK VAL'
                            comp_details[__tag_name] = h.extract_nbr(__tag_val)
                        elif __tag_name == 'PE Ratio TTM':
                            __tag_name = 'P/E'
                            comp_details[__tag_name] = h.extract_nbr(__tag_val)
                        elif __tag_name == 'EPS TTM':
                            __tag_name = 'EPS'
                            comp_details[__tag_name] = h.extract_nbr(__tag_val)
                        elif __tag_name == 'Market Cap':
                            __tag_name = 'MARKET CAP'
                            comp_details[__tag_name] = h.extract_nbr(__tag_val)
                        elif __tag_name == 'Dividend Yield':
                            __tag_name = 'DIVIDEND'
                            comp_details[__tag_name] = h.extract_nbr(__tag_val)

        else:
            print("NONE URL = ", cmp_url)
    except Exception as err:
        raise err
    return {cmp_name: comp_details}


def process_page(url_que, d_queue, g_queue, failed_que, l):
    # l.acquire()
    try:
        while True:
            try:
                '''
                    try to get task from the queue. get_nowait() function will 
                    raise queue.Empty exception if the queue is empty. 
                    queue(False) function would do the same task also.
                '''
                data = url_que.get_nowait()
            except queue.Empty:
                break
            else:
                '''
                    if no exception has been raised, add the task completion 
                    message to task_that_are_done queue
                '''
                data_lst = data.split("###")
                if len(data_lst) == 2:
                    cmp_name, cmp_url = data.split("###")[0], data.split("###")[1]
                    try:
                        if "Growth" in cmp_url:
                            # Get the EPS growth for last 5 years
                            eps_data = h.get_eps_data(cmp_url)
                            if eps_data:
                                # print("EPS Growth DATA = ", eps_data)
                                g_queue.put({cmp_name: eps_data})
                            else:
                                failed_que.put(data)
                        else:
                            result = mny_ctr_shr_frm_url(cmp_name, cmp_url)
                            if result:
                                # print("Company Details = ", result)
                                d_queue.put(result)
                            else:
                                failed_que.put(data)

                    except Exception as err:
                        # log.exception("FAILED = {}".format(err))
                        failed_que.put(data)
                else:
                    print("DATA URL =", data)
                # print("END=", data + ' is done by ' + current_process().name)

    finally:
        # l.release()
        return True


if __name__ == "__main__":
    lock = Lock()
    get_shares_details(URL, lock, c.THREAD_COUNT)
    # mny_ctr_shr_frm_url("20 Microns Limited", "https://www.investello.com/Analysis/Dashboard/20MICRONS")
