import time
from queue import Queue
import threading, logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import pandas as pd

logger.init("MC Reader", c.INFO)
log = logging.getLogger("MC Reader")

print_lock = threading.Lock()


def get_shares_details(stock_url, thread_count):
    # Variables declaration
    results_que = Queue()
    failed_que = Queue()

    start = time.time()
    # Get the shares from money control
    shares = get_shrs_from_mnctl(stock_url)
    log.info("Total number of shares returned = {}".format(len(shares)))
    sub_shares = {k: shares[k] for k in list(shares)[:100]}

    if sub_shares and len(sub_shares) > 0:
        # put into Queue
        url_que = get_shares_category(sub_shares)
        log.info("Shares added to Queue to process...")

    for i in range(thread_count):
        t = threading.Thread(target=process_queue, args=(url_que, results_que, failed_que))
        t.daemon = True
        t.start()

    url_que.join()
    log.info("Failed url count = {}".format(failed_que.qsize()))
    log.info("Success url count = {}".format(results_que.qsize()))

    while not failed_que.empty():
        log.warning("Failed URL details = {}".format(failed_que.get()))

    final_data = []
    while not results_que.empty():
        final_data.append(results_que.get())
        # tmp_dict = results_que.get()
        # key = tmp_dict.get("CATEGORY")
        # h.upd_dic_with_sub_list(key, tmp_dict, final_data)
    pd.set_option('display.max_columns', 15)
    df = pd.DataFrame(final_data)
    df = df.set_index("NAME")
    sub_groups = df.groupby("CATEGORY")
    for name, group in sub_groups:
        print(name)
        print(group)
    sorted_df = df.sort_values(by="P/E", kind="mergesort")
    print(sorted_df)
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
    with print_lock:
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


def get_shares_category(shares):
    queue = Queue()
    for title, link in shares.items():
        queue.put("###".join([title, link]))
    return queue


def process_queue(inUrlQue, resultsQue, failedQue):
    while True:
        data = inUrlQue.get()
        cmp_name, cmp_url = data.split("###")[0], data.split("###")[1]
        try:
            result = mny_ctr_shr_frm_url(cmp_name, cmp_url)
            if result:
                resultsQue.put(result)
        except Exception as e:
            failedQue.put(data)

        inUrlQue.task_done()


if __name__ == "__main__":
    get_shares_details(c.URL, c.THREAD_COUNT)