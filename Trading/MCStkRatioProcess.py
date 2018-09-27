import commons, os, time
from commons import Helper as h
from commons import Constants as c
from pandas import Series
from dao import PostgreSQLCon as db
from psycopg2.extras import execute_batch
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
import traceback

result_list = []
THREAD_COUNT = 8


def ini_stk_ratio_dic(ratio_dict, size):
    ratio_dict['EPS'] = [0.0] * size
    ratio_dict['BOOK_VAL'] = [0.0] * size
    ratio_dict['DIVIDEND'] = [0.0] * size
    ratio_dict['RET_ON_EQ'] = [0.0] * size
    ratio_dict['PRC_TO_BOOK'] = [0.0] * size
    ratio_dict['PRC_TO_SALE'] = [0.0] * size
    ratio_dict['CURR_RATIO'] = [0.0] * size
    ratio_dict['DEBT_EQUITY'] = [0.0] * size


def get_ratios_from_mc(url):
    # url = "https://www.moneycontrol.com/financials/abhinavleasingfinance/ratiosVI/ALF03#ALF03"
    data_frame = None
    try:
        bs = h.parse_url(url)
        if bs:
            cmp_name = None
            title_data = bs.find('div', {'id': 'nChrtPrc'}).find('h1', {'class': 'b_42 PT20'})
            if title_data:
                cmp_name = title_data.text.strip()
            std_data = bs.find('div', {'class': 'PB10'}).find('div', {'class': 'FL gry10'})
            if std_data:
                header_parts = std_data.text.split("|")
                nse_code, isin, sector = None, None, None
                for part in header_parts:
                    name, val = part.split(":")[0].strip(), part.split(":")[1].strip()
                    if name == 'ISIN': isin = val
                    if name == 'NSE': nse_code = val
                    if name == 'SECTOR': sector = val
                # print("nse code = {}, isin = {}, sector = {}".format(nse_code, isin, sector))
                if not nse_code: nse_code = isin

                data = [
                    [
                        [td.string.strip() for td in tr.find_all('td') if td.string]
                        for tr in table.find_all('tr')[2:]
                    ]
                    for table in bs.find_all("table", {"class": "table4"})[2:]
                ]

                if data and len(data) > 0:
                    STK_RATIO_ELEMENTS = {}
                    ele_list = data[0]
                    STK_RATIO_ELEMENTS['STK_YEAR'] = data[0][0]
                    ini_stk_ratio_dic(STK_RATIO_ELEMENTS, len(STK_RATIO_ELEMENTS['STK_YEAR']))
                    i = 2
                    while i < len(ele_list) - 4:
                        arr = ele_list[i]
                        if len(arr) > 5:
                            key = c.STK_RATIO_CON.get(arr[0])
                            val = arr[1:]
                            if key: STK_RATIO_ELEMENTS[key] = val
                        i += 1
                    print("STK RATIO = {} of Processing URL = {}".format(STK_RATIO_ELEMENTS, url))
                    data_frame = get_data_frame(nse_code, sector, cmp_name, STK_RATIO_ELEMENTS)
                    data_frame.drop_duplicates(subset=['STK_YEAR', 'NSE_CODE'], inplace=True)
                else:
                    print("Key ratios are not listed to {}".format(url))

    except Exception as err:
        print("Error while parsing URL in get_ratio function = ", str(err))

    return data_frame


def get_data_frame(nse_code, sector, cmp_name, data):
    # Set pandas options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', 0)
    df = pd.DataFrame(data)
    sector_parts = sector.split("-")
    sector = sector_parts[0]
    sub_sector = sector_parts[1] if len(sector_parts) > 1 else 'NA'
    df = df.assign(NSE_CODE=Series(nse_code, index=df.index),
                   SECTOR=sector, SUB_SECTOR=sub_sector, NAME=cmp_name)
    drop_cols = ['STK_YEAR', 'NSE_CODE', 'SECTOR', 'NAME', 'SUB_SECTOR']
    cols = df.columns.drop(drop_cols)
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    df = df.fillna(0)

    return df


def process_pages(page_list):
    df_frames = []
    for line in page_list:
        df = get_ratios_from_mc(line)
        if df is not None and isinstance(df, pd.DataFrame) \
                and not df.empty:
            df_frames.append(df)

    return df_frames


def log_result(result):
    if len(result) > 0:
        result_list.append(result)


def load_stk_ratio():
    # Variables declaration
    start = time.time()
    file_path = os.path.join(commons.get_prop('base-path', 'ratio-input'))
    files = [os.path.join(file_path, fn) for fn in next(os.walk(file_path))[2]]
    all_pages = []
    try:
        for file in files:
            read_lines = h.read_list_from_json_file(file)
            all_pages.extend(read_lines)

        # Total number of links to process
        print("No of urls to process", len(all_pages))
        page_bins = h.chunks(THREAD_COUNT, all_pages)

        pool = ThreadPool(processes=THREAD_COUNT)
        # use all available cores, otherwise specify the number you want as an argument
        for link_array in page_bins:
            pool.apply_async(process_pages, args=(link_array,), callback=log_result)
        pool.close()
        pool.join()

        for df_frames in result_list:
            try:
                result = pd.concat(df_frames, ignore_index=True)
                if len(result) > 0:
                    df_columns = list(result)
                    table = "STK_PERF_HISTORY"
                    values = "to_date(%s, 'MONYY'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
                    constraint = ', '.join(['NAME', 'NSE_CODE', 'STK_YEAR'])
                    # create INSERT INTO table (columns) VALUES('%s',...)
                    insert_stmt = h.create_update_query(table, df_columns, values, constraint)
                    curr, con = db.get_connection()
                    execute_batch(curr, insert_stmt, result.values)
                    con.commit()
                    db.close_connection(con, curr)

            except Exception as err:
                print("Exception while inserting data into table ", str(err))

    except Exception as err:
        print(str(err))
    print("Execution time = {0:.5f}".format(time.time() - start))


if __name__ == "__main__":
    load_stk_ratio()
    # get_ratios_from_mc("https://www.moneycontrol.com/financials/erislifesciences/ratiosVI/EL01#EL01")
