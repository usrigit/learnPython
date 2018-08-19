import pandas as pd
import commons, os
import time
import logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import matplotlib.pyplot as plt

URL = commons.get_prop('common', 'ipo-url')
PROCESS_COUNT = commons.get_prop('common', 'process_cnt')
logger.init("IPOMC Reader", c.INFO)
log = logging.getLogger("IPOMC Reader")


def get_listed_ipo(stock_url, pc_cnt):
    # Variables declaration
    start = time.time()
    # Get the shares from money control
    shares = get_shrs_from_mnctl(stock_url)
    log.info("Total number of shares returned = {}".format(len(shares)))
    sub_shares = {k: shares[k] for k in list(shares)[:100]}

    if sub_shares and len(sub_shares) > 0:
        print(sub_shares)
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
            table = bs.find("table", {"class": "tablesorter"})
            rows = table.findAll("tr")
            ths = rows[1].find_all('th')
            heads = [ele.text[:12].strip() for ele in ths]
            data = []
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                if len(cols) == 0:
                    continue
                data.append([ele for ele in cols if ele])
        df = pd.DataFrame(data, columns=heads)
        pd.set_option('display.max_columns', 15)
        df = df.set_index("IPO Name")
        # sliced_df = df.loc[:, ["Date", "Issue", "Listing Open", "Listing Clos", "Listing", "CMP", "Current"]]
        sliced_df = df.loc[:, ["Date", "Issue", "Listing", "CMP", "Current"]]
        sliced_df["Current"] = sliced_df["Current"].astype(float)
        sorted_df = sliced_df.sort_values(by='Current', ascending=False)
        sub_df = sorted_df.query('Current > 0')
        writer_orig = pd.ExcelWriter(os.path.join(commons.get_prop('base-path', 'output'), 'IOP_Listings.xlsx'),
                                     engine='xlsxwriter')
        sub_df.to_excel(writer_orig, index=False, sheet_name='report')
        writer_orig.save()
        print("Writing completed")

        # print(sorted_df)



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
            print("COMP DETAILS =", comp_details)

    except Exception as err:
        log.err("mny_ctr_shr_frm_url ERROR = ", str(err))
        raise err
    return comp_details


if __name__ == "__main__":
    get_listed_ipo(URL, PROCESS_COUNT)
