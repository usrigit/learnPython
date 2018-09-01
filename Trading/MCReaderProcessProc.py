import time
import multiprocessing as multi, logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import pandas as pd
import commons, os, numpy as np
from queue import Queue

logger.init("MC Reader Process Proc", c.INFO)
log = logging.getLogger("MC Reader Process Proc")


def chunks(n, page_list):
    """Splits the list into n chunks"""
    return np.array_split(page_list, n)


def get_list_of_share_links(stock_url):
    shares = get_shrs_from_mnctl(stock_url)
    return ["###".join([title, link]) for title, link in shares.items() if title and link]


def get_shares_details(stock_url, process_cnt):
    # Variables declaration
    jobs = []
    spipe_list = []

    failed_que = multi.Queue()
    start = time.time()
    all_pages = []
    # Get the shares from money control
    # for one in range(97, 123):
    #     url = stock_url + "/" + chr(one).upper()
    #     page_list = get_list_of_share_links(url)
    #     all_pages.extend(page_list)
    # all_pages = all_pages[:1]
    all_pages = ['Yes Bank###https://www.moneycontrol.com/india/stockpricequote/textiles-spinning-cotton-blended/aptyarns/APT01']
    # cpdus = multi.cpu_count()
    cpdus = 1
    print("Total Process count = {}".format(cpdus))
    print("Total URL count = {}".format(len(all_pages)))
    page_bins = chunks(cpdus, all_pages)

    for cpdu in range(cpdus):
        recv_end, send_end = multi.Pipe(False)
        worker = multi.Process(target=process_page, args=(page_bins[cpdu], send_end, failed_que,))
        jobs.append(worker)
        spipe_list.append(recv_end)
        worker.start()

    for proc in jobs:
        proc.join()

    result_list = [x.recv() for x in spipe_list]

    while not failed_que.empty():
        print("Failed URL = ", failed_que.get())
    print("Final results list = ", len(result_list))
    final_data = {}
    for results in result_list:
        print("size of the results array from result_list = ", len(results))
        for tmp_dict in results:
            key = tmp_dict.get("CATEGORY")
            h.upd_dic_with_sub_list(key, tmp_dict, final_data)
    print("Size of the FINAL dictionary = ", len(final_data))
    pd.set_option('display.max_columns', 15)

    for category in final_data:
        cat_up = category.upper()
        print("CATEGORY = {} and count = {}".format(cat_up, len(final_data[category])))
        df = pd.DataFrame(final_data[category])
        df = df.set_index("NAME")
        # Slice it as needed
        sliced_df = df.loc[:, ['MARKET CAP (Rs Cr)', 'EPS (TTM)', 'P/E', 'INDUSTRY P/E', 'BOOK VALUE (Rs)',
                               'FACE VALUE (Rs)', 'DIV YIELD.(%)']]
        filtered_df = sliced_df[sliced_df['EPS (TTM)'] != '-']
        filtered_df = filtered_df.apply(pd.to_numeric, errors='ignore')
        sorted_df = filtered_df.sort_values(by=['EPS (TTM)', 'P/E'], ascending=[False, False])
        writer_orig = pd.ExcelWriter(os.path.join(commons.get_prop('base-path', 'output'), cat_up + '_Listings.xlsx'),
                                     engine='xlsxwriter')
        sorted_df.to_excel(writer_orig, index=True, sheet_name='report')
        writer_orig.save()
    print("Execution time = {0:.5f}".format(time.time() - start))


def get_shrs_from_mnctl(url):
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
            table = bs.find("table", {"class": "pcq_tbl MT10"})
            for row in table.findAll("tr"):
                for link in row.findAll("a"):
                    shares[link.get("title")] = link.get('href')
    except Exception as err:
        log.exception("ERROR in get_shr_from_mnctl = {}".format(err))
    return shares


def get_daily_st_details(bs, id):
    bse_data = bs.find('div', {'id': id}).find('div', {'class': 'brdb PB5'}).findAll('div')
    bse_dt = bse_data[3].text
    bse_st_price = bse_data[4].text
    bse_st_vol = h.alpnum_to_num(bse_data[6].text.strip().split("\n")[0])
    print("bse dt", bse_dt)
    print("bse_st_price", bse_st_price)
    print("bse_st_vol", bse_st_vol)

    bse_data = bs.find('div', {'id': 'content_bse'}).find('div', {'class': 'brdb PA5'}).findAll('div')
    stk_prc = 0
    prev_close = 0
    open_price = 0
    for ele in bse_data:
        if ele.get("class") == ['gD_12', 'PB3']:
            if stk_prc == 0:
                prev_close = ele.text.strip()
            elif stk_prc == 1:
                open_price = ele.text.strip()
            stk_prc += 1
    print("prev_close", prev_close)
    print("open_price", open_price)
    bse_data = bs.find('div', {'id': 'content_bse'}).find('div', {'class': "PT10 clearfix"}).findAll('div')
    stk_p = 0
    tl_price = 0
    th_price = 0
    wl_price = 0
    wh_price = 0
    for ele in bse_data:
        if ele.get('class') == ["PB3", "gD_11"]:
            ele_li = ele.text.strip().split("\n")
            if stk_p == 0:
                tl_price = ele_li[1]
                th_price = ele_li[3]
            elif stk_p == 1:
                wl_price = ele_li[1]
                wh_price = ele_li[3]
            stk_p += 1
    print("tl_price", tl_price)
    print("th_price", th_price)
    print("wl_price", wl_price)
    print("wh_price", wh_price)


def mny_ctr_shr_frm_url(cmp_name, cmp_url):
    comp_details = {}
    try:
        comp_details['NAME'] = cmp_name
        elements = cmp_url.split("/")
        if len(elements) > 5:
            key = elements[5]
            comp_details['CATEGORY'] = key
        bs = h.parse_url(cmp_url)
        if bs:
            base_data = bs.find('div', {'class': 'FL gry10'})
            bs_txt_arr = base_data.text.split("|")
            bse_code = bs_txt_arr[0].split(":")[1]
            nse_code = bs_txt_arr[1].split(":")[1].strip()
            isin_code = bs_txt_arr[2].split(":")[1].strip()
            sector = bs_txt_arr[3].split(":")[1]

            print("bse_code", bse_code)
            print("nse_code", nse_code)
            print("isin_code", isin_code)
            if nse_code:
                get_daily_st_details(bs, 'content_nse')
            elif isin_code:
                get_daily_st_details(bs, 'content_bse')
            std_data = bs.find('div', {'id': 'mktdet_1'})
            for each_div in std_data.findAll('div', attrs={'class': 'PA7 brdb'}):
                sub_div = each_div.descendants
                __tag_name, __tag_value = None, None
                for cd in sub_div:
                    if cd.name == 'div' and cd.get('class', '') == ['FL', 'gL_10', 'UC']:
                        __tag_name = cd.text
                    if cd.name == 'div' and cd.get('class', '') == ['FR', 'gD_12']:
                        __tag_value = cd.text
                    if __tag_name and __tag_value:
                        comp_details[__tag_name] = __tag_value
                        __tag_name, __tag_value = None, None
            print("COMP DETAILS =", comp_details)

    except Exception as err:
        # log.error("mny_ctr_shr_frm_url ERROR = ", str(err))
        raise err
    return comp_details


def process_page(data_array, send_end, failed_que):
    results = []
    print("DATA ARRAY = ", data_array)
    for data in data_array:
        print("DATA = ", data)
        cmp_name, cmp_url = data.split("###")[0], data.split("###")[1]
        try:
            result = mny_ctr_shr_frm_url(cmp_name, cmp_url)
            if result:
                print("Result = ", result)
                results.append(result)
            else:
                failed_que.put(data)
        except Exception as err:
            failed_que.put(data)
    # After all data completed then send
    print("len of the results list = ", len(results))
    send_end.send(results)

                    
if __name__ == "__main__":

    get_shares_details(c.URL, c.THREAD_COUNT)
                    

                    
                    
