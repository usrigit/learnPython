import threading
from queue import Queue
import numpy as np
import multiprocessing as multi
import time
import traceback
import pandas as pd
from commons import Helper as h


"""
parallel IO operations are possible using threads. 
This is because performing IO operations releases the GIL
"""
print_lock = threading.Lock()


def mny_ctr_shr_frm_url(cmp_name, cmp_url, cat):
    with print_lock:
        # print("\nStarting thread {}".format(threading.current_thread().name))
        try:
            dict_comp = {"NAME": cmp_name}

            bs = h.parse_url(cmp_url)

            if bs:
                print("bs coming")
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
                            dict_comp[__tag_name] = __tag_value
                            __tag_name, __tag_value = None, None
            return dict_comp

        except Exception as err:
            print("Error is = ", str(err))
            raise err

        return


def process_queue(url_queue, results, failed_queue):
    while True:
        data = url_queue.get()
        print("DATA = ", data)
        cmp_name, cmp_url, cat = data.split("###")[0], data.split("###")[1], data.split("###")[2]
        try:
            result = mny_ctr_shr_frm_url(cmp_name, cmp_url, cat)
            results.put(result)
            print("Results size = ", results.qsize())
        except Exception as err:
            print("Error from mny_ctr_shr_frm_url", err)
            failed_queue.put(data)
        url_queue.task_done()


def get_shr_dtls():
    start = time.time()
    url_queue = Queue()
    results = Queue()
    failed_que = Queue()
    url_queue.put(
        '3M India' + "###" + 'http://www.moneycontrol.com/india/stockpricequote/diversified/3mindia/MI42' + "###" + "A")
    url_queue.put(
        'Balmer Lawrie' + "###" + 'http://www.moneycontrol.com/india/stockpricequote/diversified/balmerlawriecompany/BLC' + "###" + "A")
    url_queue.put(
        'Century' + "###" + 'http://www.moneycontrol.com/india/stockpricequote/diversified/centurytextilesindustries/CTI' + "###" + "A")
    url_queue.put(
        'DCM Shriram' + "###" + 'http://www.moneycontrol.com/india/stockpricequote/diversified/dcmshriram/DCM02' + "###" + "B")
    url_queue.put(
        'Grasim' + "###" + 'http://www.moneycontrol.com/india/stockpricequote/diversified/grasimindustries/GI01' + "###" + "B")
    url_queue.put(
        'Kesoram' + "###" + 'http://www.moneycontrol.com/india/stockpricequote/diversified/kesoramindustries/KI08' + "###" + "B")
    cpus = multi.cpu_count()


    for i in range(4):
        t = threading.Thread(target=process_queue, args=(url_queue, results, failed_que))
        t.daemon = True
        t.start()

    url_queue.join()
    while not failed_que.empty():
        print("FAILED URL = ", failed_que.get())
    final_data = []
    while not results.empty():
        data = results.get()
        print("COMPANY = ", data)
        final_data.append(data)
    print("Execution time = {0:.5f}".format(time.time() - start))

    df = pd.DataFrame(final_data)
    df = df.set_index("NAME")
    pd.set_option('display.max_columns', 15)
    sorted_df = df.sort_values(by="P/E", kind="mergesort")

    print("DATA FRAME =============== \n ")
    print(df)
    print("Sorted one \n")
    print(sorted_df)


def chunks(n, page_list):
    """Splits the list into n chunks"""
    return np.array_split(page_list,n)


if __name__ == "__main__":
    print("CPU count = ", multi.cpu_count())
    get_shr_dtls()
