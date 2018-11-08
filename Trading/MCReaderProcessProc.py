import time, traceback
import multiprocessing as multi, logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import commons, os, numpy as np
from psycopg2.extras import execute_batch
import pandas as pd
from dao import PostgreSQLCon as db

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
    try:
        result_list = [x.recv() for x in spipe_list]
        final_data = {}
        ratio_links = []
        print("FAILED URL COUNT = {}".format(failed_que.qsize()))
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

        # Set pandas options
        pd.set_option('display.max_columns', None)
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('max_colwidth', 0)
        for category in final_data:
            df = pd.DataFrame(final_data[category])
            cols = df.columns.drop(['STK_DATE', 'NSE_CODE', 'NAME', 'CATEGORY', 'SUB_CATEGORY', 'URL'])
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            df = df.fillna(0)
            # print(df)
            if len(df) > 0:
                try:
                    df_columns = list(df)
                    table = "STK_DETAILS"
                    columns = ",".join(df_columns)
                    print("Batch started with count {} to insert into DB = ", len(df.values))
                    values = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                             "%s, %s, %s, %s, %s, %s, to_date(%s, 'YYYMONDD'), %s, %s, %s);"
                    # create INSERT INTO table (columns) VALUES('%s',...)
                    insert_stmt = "INSERT INTO {} ({}) VALUES {}".format(table, columns, values)
                    curr, con = db.get_connection()
                    execute_batch(curr, insert_stmt, df.values)
                    con.commit()
                    db.close_connection(con, curr)
                    print("Batch inserted into DB successfully")

                except Exception as err:
                    print("While inserting data into DB exception = {}".format(err))

    except Exception as err:
        print("Exception in get_share_details function = {}".format(err))

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
                # print("BFR tag Name = {} and value = {}".format(__tag_name, __tag_value))
                if __tag_name and __tag_value and __tag_name in c.STK_RATIO_CON:
                    __tag_name = c.STK_RATIO_CON[__tag_name]
                    if __tag_name not in ['NAME', 'CATEGORY', 'SUB_CATEGORY']:
                        __tag_value = h.extract_float(__tag_value)
                    # print("AFR tag Name = {} and value = {}".format(__tag_name, __tag_value))
                    comp_details[__tag_name] = __tag_value
                    __tag_name, __tag_value = None, None
        # print("COMP DETAILS =", comp_details)

    except Exception as err:
        print("While parsing PERF DETAILS {}".format(err))
        traceback.print_exc()
    return comp_details


def mc_get_day_stk_details(bs, id, cmp_url):
    data_dict = {}
    try:
        # Get date, stock current price and traded volume
        if bs.find('div', {'id': id}).find('div', {'class': 'brdb PB5'}):
            bse_data = bs.find('div', {'id': id}).find('div', {'class': 'brdb PB5'}).findAll('div')
            if bse_data:
                year = time.strftime("%Y")
                bse_dt = bse_data[3].text.split(",")[0].strip()
                if bse_dt and len(bse_dt) > 5:
                    data_dict["STK_DATE"] = year + ' ' + bse_dt
                data_dict["CURR_PRICE"] = bse_data[4].text.strip()
                bse_st_vol = h.alpnum_to_num(bse_data[6].text.strip().split("\n")[0])
                data_dict["TRADED_VOLUME"] = bse_st_vol.strip()
        # Get previous and open price of the share
        if bs.find('div', {'id': id}).find('div', {'class': 'brdb PA5'}):
            bse_data = bs.find('div', {'id': id}).find('div', {'class': 'brdb PA5'}).findAll('div')
            stk_prc = 0
            for ele in bse_data:
                if ele.get("class") == ['gD_12', 'PB3']:
                    if stk_prc == 0:
                        data_dict["PREV_PRICE"] = ele.text.strip()
                    elif stk_prc == 1:
                        data_dict["OPEN_PRICE"] = ele.text.strip()
                    stk_prc += 1
        # Get low, high and 52 week prices
        if bs.find('div', {'id': id}).find('div', {'class': "PT10 clearfix"}):
            bse_data = bs.find('div', {'id': id}).find('div', {'class': "PT10 clearfix"}).findAll('div')
            stk_p = 0
            for ele in bse_data:
                if ele.get('class') == ["PB3", "gD_11"]:
                    ele_li = ele.text.strip().split("\n")
                    if stk_p == 0:
                        data_dict["LOW_PRICE"] = ele_li[1]
                        data_dict["HIGH_PRICE"] = ele_li[3]
                    elif stk_p == 1:
                        data_dict["LOWEST_PRICE"] = ele_li[1]
                        data_dict["HIGEST_PRICE"] = ele_li[3]
                    stk_p += 1
        if not (len(data_dict)) == 9:
            data_dict = {}
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
                sector = bs_txt_arr[3].split(":")[1].strip()
                stk_result = {}
                if nse_code:
                    stk_result = mc_get_day_stk_details(bs, 'content_nse', cmp_url)
                    # print("STK DAY details NSE = ", stk_result)
                if not stk_result and isin_code:
                    stk_result = mc_get_day_stk_details(bs, 'content_bse', cmp_url)
                    # print("STK DAY details BSE  = ", stk_result)
                if stk_result:
                    comp_details = mc_get_perf_stk_details(bs)
                    comp_details['NAME'] = cmp_name
                    category = 'N/A'
                    sub_category = 'N/A'
                    if sector:
                        cat_list = sector.split("-")
                        if len(cat_list) > 1:
                            category = cat_list[0]
                            sub_category = cat_list[1:]
                        else:
                            category = sector
                    comp_details['CATEGORY'] = category
                    comp_details['SUB_CATEGORY'] = sub_category
                    nse_code = nse_code if nse_code else isin_code
                    comp_details['NSE_CODE'] = nse_code
                    comp_details['URL'] = cmp_url
                    comp_details.update(stk_result)
                    # print("STK complete details = ", comp_details)
                else:
                    print("COMP {} not listed or errored".format(cmp_url))
            else:
                print("COMP {} not listed or errored".format(cmp_url))
    except Exception as err:
        print("CMP URL = {} with error = {}".format(cmp_url, err))
        # raise err
    return comp_details


def process_page(data_array, send_end, failed_que):
    results = []
    # print("Size of the data array from current process {} is {} "
    # .format(multi.current_process().name, len(data_array)))
    for data in data_array:
        cmp_name, cmp_url = data.split("###")[0], data.split("###")[1]
        # print("URL = {} processed by {} ".format(cmp_url, multi.current_process().name))
        try:
            result = mny_ctr_shr_frm_url(cmp_name, cmp_url)
            if result:
                # print("{} process output Result = {}".format(multi.current_process().name, result.get("NAME")))
                results.append(result)
            else:
                # print("{} Parsing failed url {}= ".format(multi.current_process().name, cmp_url))
                failed_que.put(data)
        except Exception as err:
            print("Failed exception = ", str(err))
            failed_que.put(data)
    # After all data completed then send
    # print("Sending results {} to Main process from {}".format(len(results), multi.current_process().name))
    # print("Error results {} to Main process from {}".format(failed_que.qsize(), multi.current_process().name))
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
        # all_pages = all_pages[:100]
        # all_pages = ['A###https://www.moneycontrol.com/india/stockpricequote/miscellaneous/bhagyanagarproperties/BP13',
        #              'B###https://www.moneycontrol.com/india/stockpricequote/miscellaneous/amjumbobags/JB03']
        # all_pages = get_list_of_share_links(stock_url)
        get_shares_details(all_pages, first_time)


if __name__ == "__main__":
    get_list_of_shares_mc(c.URL, True)
    # map = mny_ctr_shr_frm_url('yes bank',
    #                      'https://www.moneycontrol.com/india/stockpricequote/miscellaneous/bhagyanagarproperties/BP13')
    # df = pd.DataFrame(map)
    # print(df)