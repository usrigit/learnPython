import time
from queue import Queue
import threading, logging
from commons import Helper as h
from commons import Constants as c
from commons import logger
import pandas as pd
from dao import PostgreSQLCon as db
from psycopg2.extras import execute_batch
from Trading import MCReaderProcessProc as mcp

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
    print("Total number of shares returned = {}".format(len(shares)))
    # shares = {k: shares[k] for k in list(shares)[:50]}
    if shares and len(shares) > 0:
        # put into Queue
        url_que = get_shares_category(shares)
        print("Shares added to Queue to process...")

    for i in range(thread_count):
        t = threading.Thread(target=process_queue, args=(url_que, results_que, failed_que))
        t.daemon = True
        t.start()

    url_que.join()
    print("Failed url count = {}".format(failed_que.qsize()))
    print("Success url count = {}".format(results_que.qsize()))

    while not failed_que.empty():
        log.warning("Failed URL details = {}".format(failed_que.get()))

    final_data = {}
    while not results_que.empty():
        # final_data.append(results_que.get())
        tmp_dict = results_que.get()
        key = tmp_dict.get("CATEGORY")
        h.upd_dic_with_sub_list(key, tmp_dict, final_data)
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
                    constraint = ', '.join(['NAME', 'NSE_CODE', 'STK_DATE'])
                    print("Batch started with count {} to insert into DB = ", len(df.values))
                    values = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                             "%s, %s, %s, %s, %s, %s, to_date(%s, 'YYYMONDD'), %s, %s, %s"
                    # create INSERT INTO table (columns) VALUES('%s',...)
                    insert_stmt = h.create_update_query(table, df_columns, values, constraint)
                    curr, con = db.get_connection()
                    execute_batch(curr, insert_stmt, df.values)
                    con.commit()
                    db.close_connection(con, curr)
                    print("Batch inserted into DB successfully")

                except Exception as err:
                    print("While inserting data into DB exception = {}".format(err))

    # pd.set_option('display.max_columns', 15)
    # for category in final_data:
    #     cat_up = category.upper()
    #     print("CATEGORY = {} and count = {}".format(cat_up, len(final_data[category])))
    #     df = pd.DataFrame(final_data[category])
    #     df = df.set_index("NAME")
    #     # Slice it as needed
    #     sliced_df = df.loc[:, ['MARKET CAP (Rs Cr)', 'EPS (TTM)', 'P/E', 'INDUSTRY P/E', 'BOOK VALUE (Rs)',
    #                            'FACE VALUE (Rs)', 'DIV YIELD.(%)']]
    #     sliced_df = sliced_df.apply(pd.to_numeric, errors='ignore')
    #     sorted_df = sliced_df.sort_values(by=['EPS (TTM)', 'P/E'], ascending=[False, False])
    #     writer_orig = pd.ExcelWriter(os.path.join(commons.get_prop('base-path', 'output'), cat_up + '_Listings.xlsx'),
    #                                  engine='xlsxwriter')
    #     sorted_df.to_excel(writer_orig, index=True, sheet_name='report')
    #     writer_orig.save()

        # Sort by  P/E

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
            comp_details = mcp.mny_ctr_shr_frm_url(cmp_name, cmp_url)
            print("processing url {} with result = {}".format(cmp_url, comp_details))
        except Exception as err:
            print("mny_ctr_shr_frm_url ERROR = ", str(err))
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
