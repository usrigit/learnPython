import time, re
import multiprocessing as multi, logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import pandas as pd
import commons, os, numpy as np

logger.init("Investello Reader Process Proc", c.INFO)
log = logging.getLogger("Investello Reader Process Proc")

URL = "https://www.investello.com/Companies"

def chunks(n, page_list):
    """Splits the list into n chunks"""
    return np.array_split(page_list, n)


def get_list_of_share_links(stock_url):
    shares = get_shrs_from_mnctl_div(stock_url)
    return ["###".join([title, link]) for title, link in shares.items() if title and link]


def get_shrs_from_mnctl_div(url):
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


def get_shares_details(stock_url):
    # Variables declaration
    jobs = []
    spipe_list = []

    failed_que = multi.Queue()
    start = time.time()
    all_pages = []
    # Get the shares from Investello
    all_pages = get_list_of_share_links(stock_url)
    cpdus = 50
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
    print(result_list)

    # while not failed_que.empty():
    #     print("Failed URL = ", failed_que.get())
    # print("Final results list = ", len(result_list))
    # final_data = {}
    # for results in result_list:
    #     print("size of the results array from result_list = ", len(results))
    #     for tmp_dict in results:
    #         key = tmp_dict.get("CATEGORY")
    #         h.upd_dic_with_sub_list(key, tmp_dict, final_data)
    # print("Size of the FINAL dictionary = ", len(final_data))
    # pd.set_option('display.max_columns', 15)
    #
    # for category in final_data:
    #     cat_up = category.upper()
    #     print("CATEGORY = {} and count = {}".format(cat_up, len(final_data[category])))
    #     df = pd.DataFrame(final_data[category])
    #     df = df.set_index("NAME")
    #     # Slice it as needed
    #     sliced_df = df.loc[:, ['MARKET CAP', 'EPS', 'P/E', 'INDUSTRY P/E', 'BOOK VALUE (Rs)',
    #                            'FACE VALUE (Rs)', 'DIV YIELD.(%)']]
    #     filtered_df = sliced_df[sliced_df['EPS (TTM)'] != '-']
    #     filtered_df = filtered_df.apply(pd.to_numeric, errors='ignore')
    #     sorted_df = filtered_df.sort_values(by=['EPS (TTM)', 'P/E'], ascending=[False, False])
    #     writer_orig = pd.ExcelWriter(os.path.join(commons.get_prop('base-path', 'output'), cat_up + '_Listings.xlsx'),
    #                                  engine='xlsxwriter')
    #     sorted_df.to_excel(writer_orig, index=True, sheet_name='report')
    #     writer_orig.save()
    print("Execution time = {0:.5f}".format(time.time() - start))


def mny_ctr_shr_frm_url(cmp_name, cmp_url):
    comp_details = {}
    try:
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

            # Get the EPS growth for last 5 years
            link = cmp_url.replace('Dashboard', 'Growth')
            eps_data = h.get_eps_data(link)
            comp_details.update(eps_data)
            # print("COMP DETAILS =", comp_details)
    except Exception as err:
        # log.error("mny_ctr_shr_frm_url ERROR = ", str(err))
        raise err
    return comp_details


def process_page(data_array, send_end, failed_que):
    results = []
    for data in data_array:
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
    get_shares_details(URL)
    # mny_ctr_shr_frm_url("20 Microns Limited", "https://www.investello.com/Analysis/Dashboard/20MICRONS")
                    

                    
                    
