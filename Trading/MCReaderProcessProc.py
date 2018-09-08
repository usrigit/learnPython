import time, traceback
import multiprocessing as multi, logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import pandas as pd
import commons, os, numpy as np
import json

logger.init("MC Reader Process Proc", c.INFO)
log = logging.getLogger("MC Reader Process Proc")


def chunks(n, page_list):
    """Splits the list into n chunks"""
    return np.array_split(page_list, n)


def get_list_of_share_links(stock_url):
    shares = get_shrs_from_mnctl(stock_url)
    return ["###".join([title, link]) for title, link in shares.items() if title and link]


def define_mc_ratio_link(url):
    ele_list = url.split("/")
    code = ele_list[len(ele_list) - 1]
    name = ele_list[len(ele_list) - 2]
    return "https://www.moneycontrol.com/financials/" + name + "/ratiosVI/" + code + "#" + code


def get_shares_details(all_pages, first_time_process):
    # Variables declaration
    jobs = []
    spipe_list = []
    failed_que = multi.Queue()
    start = time.time()
    cpdus = multi.cpu_count()
    print("Total Process count = {}".format(cpdus))
    print("Total URL count = {}".format(len(all_pages)))
    page_bins = chunks(cpdus, all_pages)
    for cpdu in range(cpdus):
        recv_end, send_end = multi.Pipe(False)
        worker = multi.Process(target=process_page, args=(page_bins[cpdu], send_end, failed_que,))
        worker.daemon = True
        jobs.append(worker)
        spipe_list.append(recv_end)
        worker.start()

    # end_at = time.time() + (5)
    # while jobs:
    #     job = jobs.pop()
    #     delta = end_at - time.time()
    #     if delta > 0:
    #         job.join(timeout=delta)
    #     job.terminate()
    #     job.join()
    for job in jobs:
        job.join(timeout=10)
    print("All jobs completed......")

    # if first_time_process:
    #     result_list = [x.recv() for x in spipe_list]
    #     failed_pages = []
    #     while not failed_que.empty():
    #         failed_pages.append(failed_que.get())
    #     print("Parsing failed page count = {}".format(len(failed_pages)))
    #     get_shares_details(failed_pages, False)
    result_list = [x.recv() for x in spipe_list]
    final_data = {}
    ratio_links = []
    for results in result_list:
        print("size of the results array from result_list = ", len(results))
        for tmp_dict in results:
            key = tmp_dict.get("CATEGORY")
            link = tmp_dict.get("URL")
            ratio_links.append(define_mc_ratio_link(link))
            h.upd_dic_with_sub_list(key, tmp_dict, final_data)
    if ratio_links and len(ratio_links) > 0:
        print("Size of the RATIO array = ", len(ratio_links))
        h.write_list_to_json_file(os.path.join(
            commons.get_prop('base-path', 'output'), "5yrs_stk_ratio.txt"), ratio_links)

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


def mc_get_perf_stk_details(bs):
    comp_details = {}
    try:
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
        # print("COMP DETAILS =", comp_details)

    except Exception as err:
        print("While parsing PERF DETAILS {}".format(err))
    return comp_details


def mc_get_day_stk_details(bs, id):
    data_dict = {}
    try:
        if bs.find('div', {'id': id}).find('div', {'class': 'brdb PB5'}):
            bse_data = bs.find('div', {'id': id}).find('div', {'class': 'brdb PB5'}).findAll('div')
            bse_dt = bse_data[3].text
            bse_st_price = bse_data[4].text
            bse_st_vol = h.alpnum_to_num(bse_data[6].text.strip().split("\n")[0])
            data_dict["STK_DATE"] = bse_dt
            data_dict["STK_CUR_RATE"] = bse_st_price
            data_dict["STK_TRD_VOL"] = bse_st_vol

        if bs.find('div', {'id': id}).find('div', {'class': 'brdb PA5'}):
            bse_data = bs.find('div', {'id': id}).find('div', {'class': 'brdb PA5'}).findAll('div')
            stk_prc = 0
            for ele in bse_data:
                if ele.get("class") == ['gD_12', 'PB3']:
                    if stk_prc == 0:
                        data_dict["STK_PREV_RATE"] = ele.text.strip()
                    elif stk_prc == 1:
                        data_dict["STK_OPEN_RATE"] = ele.text.strip()
                    stk_prc += 1

        if bs.find('div', {'id': id}).find('div', {'class': "PT10 clearfix"}):
            bse_data = bs.find('div', {'id': id}).find('div', {'class': "PT10 clearfix"}).findAll('div')
            stk_p = 0
            for ele in bse_data:
                if ele.get('class') == ["PB3", "gD_11"]:
                    ele_li = ele.text.strip().split("\n")
                    if stk_p == 0:
                        data_dict["STK_LOW_RATE"] = ele_li[1]
                        data_dict["STK_HIGH_RATE"] = ele_li[3]
                    elif stk_p == 1:
                        data_dict["STK_YLOW_RATE"] = ele_li[1]
                        data_dict["STK_YHIGH_RATE"] = ele_li[3]
                    stk_p += 1

    except Exception as err:
        print("While prasing for day stocks = {}".format(err))
    return data_dict


def mny_ctr_shr_frm_url(cmp_name, cmp_url):
    comp_details = {}
    try:
        bs = h.parse_url(cmp_url)
        if bs:
            base_data = bs.find('div', {'class': 'FL gry10'})
            if base_data:
                bs_txt_arr = base_data.text.split("|")
                bse_code = bs_txt_arr[0].split(":")[1]
                nse_code = bs_txt_arr[1].split(":")[1].strip()
                isin_code = bs_txt_arr[2].split(":")[1].strip()
                sector = bs_txt_arr[3].split(":")[1]
                stk_result = {}
                if nse_code:
                    stk_result = mc_get_day_stk_details(bs, 'content_nse')
                    print("RESULT in NSE = ", stk_result)
                if not stk_result and isin_code:
                    print("Processing in BSE  = ", stk_result)
                    stk_result = mc_get_day_stk_details(bs, 'content_bse')
                    # print("RESULT in BSE  = ", stk_result)
                if stk_result:
                    comp_details = mc_get_perf_stk_details(bs)
                    comp_details['NAME'] = cmp_name
                    comp_details['CATEGORY'] = sector
                    comp_details['NSE_CODE'] = nse_code
                    comp_details['URL'] = cmp_url
                    comp_details.update(stk_result)

    except Exception as err:
        print("CMP URL", cmp_url)
        # raise err
    return comp_details


def process_page(data_array, send_end, failed_que):
    results = []
    print("Size of the data array from current process {} is {} ".format(multi.current_process().name, len(data_array)))
    for data in data_array:
        cmp_name, cmp_url = data.split("###")[0], data.split("###")[1]
        print("URL = {} processed by {} ".format(cmp_url, multi.current_process().name))
        try:
            result = mny_ctr_shr_frm_url(cmp_name, cmp_url)
            if result:
                print("{} process output Result = {}".format(multi.current_process().name, result.get("NAME")))
                results.append(result)
            else:
                print("{} Parsing failed url {}= ".format(multi.current_process().name, cmp_url))
                failed_que.put(data)
        except Exception as err:
            print("Failed exception = ", str(err))
            failed_que.put(data)
    # After all data completed then send
    print("Sending results {} to Main process from {}".format(len(results), multi.current_process().name))
    print("Error results {} to Main process from {}".format(failed_que.qsize(), multi.current_process().name))
    send_end.send(results)


def get_list_of_shares_mc(stock_url, first_time):
    """
    Get the list of shares from money control site
    :param stock_url:
    :return: stocks list
    """
    all_pages = []
    if stock_url:
        # Get the shares from money control
        for one in range(97, 123):
            url = stock_url + "/" + chr(one).upper()
            page_list = get_list_of_share_links(url)
            all_pages.extend(page_list)
        all_pages = all_pages[:10]
        get_shares_details(all_pages, first_time)


if __name__ == "__main__":
    get_list_of_shares_mc(c.URL, True)

