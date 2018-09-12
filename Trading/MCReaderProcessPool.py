import time
import multiprocessing as multi, logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import pandas as pd
import commons, os, numpy as np

logger.init("MC Reader Process Pool", c.INFO)
log = logging.getLogger("MC Reader Process Pool")


def get_list_of_share_links(stock_url):
    print("Parent URL = ", stock_url)
    shares = get_shrs_from_mnctl(stock_url)
    return ["###".join([title, link]) for title, link in shares.items() if title and link]


def get_shares_details(stock_url, process_cnt):
    # Variables declaration
    failed_data = []
    start = time.time()
    # Get the shares from money control
    page_list = get_list_of_share_links(stock_url)
    page_list = page_list[:10]
    print("Total Process count = {}".format(process_cnt))
    print("Total URL count = {}".format(len(page_list)))

    pool = multi.Pool(processes=process_cnt)
    # use all available cores, otherwise specify the number you want as an argument
    results = [pool.apply_async(process_queue, args=(link,)) for link in page_list]
    pool.close()
    pool.join()
    print(results)
    print("Total SUCCESS URL count = {}".format(len(results)))
    log.warning("Total FAILURE URL Count = {}".format(len(failed_data)))

    final_data = {}
    for ele in results:
        tmp_dict = ele.get()
        key = tmp_dict.get("CATEGORY")
        h.upd_dic_with_sub_list(key, tmp_dict, final_data)
    pd.set_option('display.max_columns', 15)

    for category in final_data:
        cat_up = category.upper()
        print("CATEGORY = {} and count = {}".format(cat_up, len(final_data[category])))
        df = pd.DataFrame(final_data[category])
        df = df.set_index("NAME")
        # Slice it as needed
        sliced_df = df.loc[:, ['MARKET CAP (Rs Cr)', 'EPS (TTM)', 'P/E', 'INDUSTRY P/E', 'BOOK VALUE (Rs)',
                               'FACE VALUE (Rs)', 'DIV YIELD.(%)']]
        sliced_df = sliced_df.apply(pd.to_numeric, errors='ignore')
        sorted_df = sliced_df.sort_values(by=['EPS (TTM)', 'P/E'], ascending=[False, False])
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
        log.exception("ERROR in get_shr_from_mnctl = ", str(err))
    return shares


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
        # log.error("mny_ctr_shr_frm_url ERROR = ", str(err))
        raise err
    return comp_details


def process_queue(data):
    cmp_name, cmp_url = data.split("###")[0], data.split("###")[1]
    result = None
    try:
        result = mny_ctr_shr_frm_url(cmp_name, cmp_url)
    except Exception as e:
        log.warning("FAILED DATA URL = {}".format(data))
    print("Result = > ", result)
    return result


if __name__ == "__main__":
    get_shares_details(c.URL, c.THREAD_COUNT)
