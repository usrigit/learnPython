import commons.Helper as h
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pandas import Series
from psycopg2.extras import execute_batch
import traceback, time

dict1 = {'key1': [1, 2, 3], 'key2': [11, 21, 31]}
dict2 = {'key1': [4, 5], 'key2': [41, 51]}

print({k: dict1.get(k, []) + dict2.get(k, []) for k in set().union(dict1, dict2)})

dict1 = {'key1': {'a': 1, 'b': 2}}
dict2 = {'key1': {'x': 3, 'y': 4}, 'key2': {'p': 10, 'q': 11, 'r': 12}}

final_dict = {}
for k in set().union(dict1, dict2):
    val = {}
    sub_d1 = dict1.get(k)
    sub_d2 = dict2.get(k)

    if sub_d1:
        val.update(sub_d1)

    if sub_d2:
        val.update(sub_d2)
    final_dict.update({k: val})
print("Final dict = ", final_dict)

print("Valu = ", ''.join(filter(lambda x: x.isdigit(), "VOLUME  6,438")))


def get_connection():
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(database="trading",
                                user="postgres",
                                password="postgres")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

    except psycopg2.DatabaseError as e:
        print("error creating database: %s", e)
    return cursor, conn


def fetch_data(cursor, sql):
    try:
        cursor.execute(sql)
        fetch = cursor.fetchall()
        return fetch
    except Exception as err:
        print("while fetching data from table: %s", err)


query = "INSERT INTO STK_PERF_HISTORY (NSE_CODE, EPS, BOOK_VAL, DIVIDEND, RET_ON_EQ, " \
        "PRC_TO_BOOK, PRC_TO_SALE, CURR_RATIO, DEBT_EQUITY, STK_YEAR) " \
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"


def load_data(conn, cursor, sql, parameters):
    try:
        cursor.execute(sql, parameters)
        conn.commit()
    except Exception as err:
        print("while loading data to table: %s", err)


def close_connection(conn, cursor):
    try:
        cursor.close()
        if conn is not None:
            conn.close()
    except Exception as err:
        print("Error while closing the connection: %s", err)


def create():
    """If not created, create a database with the name specified in
       the constructor"""
    conn = None
    try:
        conn = psycopg2.connect(database="trading",
                                user="postgres",
                                password="postgres")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("SELECT BOOK_VAL FROM STK_PERF_HISTORY")
        fetch = cursor.fetchall()
        print(fetch)
    except psycopg2.DatabaseError as e:
        print("error creating database: %s", e)
    finally:
        conn.close()


ratio_con = {
    'Basic EPS (Rs.)': "EPS",
    'Book Value [Excl. Reval Reserve]/Share (Rs.)': "BOOK_VAL",
    'Dividend/Share (Rs.)': "DIVIDEND",
    'Return on Equity / Networth (%)': "RET_ON_EQ",
    'Price To Book Value (X)': "PRC_TO_BOOK",
    'Price To Sales (X)': "PRC_TO_SALE",
    'Current Ratio (X)': "CURR_RATIO",
    'Total Debt/Equity (X)': "DEBT_EQUITY"
}

ratio_elements = {
    'STK_YEAR': [],
    'EPS': [],
    'BOOK_VAL': [],
    'DIVIDEND': [],
    'RET_ON_EQ': [],
    'PRC_TO_BOOK': [],
    'PRC_TO_SALE': [],
    'CURR_RATIO': [0.0, 0.0, 0.0, 0.0, 0.0],
    'DEBT_EQUITY': [0.0, 0.0, 0.0, 0.0, 0.0]
}


def parse_url():
    stock_ratio = {}
    url = "https://www.moneycontrol.com/financials/yesbank/ratiosVI/YB#YB"
    bs = h.parse_url(url)
    if bs:
        std_data = bs.find('div', {'class': 'PB10'}).find('div', {'class': 'FL gry10'})
        nse_code = (std_data.text.split("|")[1]).split(":")[1].strip()
        print("NSE_CODE", nse_code)
        data = [
            [
                [td.string.strip() for td in tr.find_all('td') if td.string]
                for tr in table.find_all('tr')[2:]
            ]
            for table in bs.find_all("table", {"class": "table4"})[2:]
        ]
        ele_list = data[0]
        ratio_elements['STK_YEAR'] = data[0][0]
        i = 2
        while i < len(ele_list) - 4:
            arr = ele_list[i]
            if len(arr) > 5:
                key = ratio_con.get(arr[0])
                val = arr[1:]
                if key: ratio_elements[key] = val
            i += 1
        print("STK RATIO = ", ratio_elements)
        # Set pandas options
        pd.set_option('display.max_columns', None)
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('max_colwidth', 0)
        df = pd.DataFrame(ratio_elements)
        df = df.apply(pd.to_numeric, errors='ignore')
        df = df.assign(NSE_CODE=Series(nse_code, index=df.index))

        if len(df) > 0:
            df_columns = list(df)
            table = "STK_PERF_HISTORY"
            values = "to_date(%s, 'MONYY'), %s, %s, %s, %s, %s, %s, %s, %s, %s"
            constraint = ', '.join(['NAME', 'NSE_CODE', 'STK_YEAR'])
            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = create_update_query(table, df_columns, values, constraint)
            print("PERF HIST= ", insert_stmt)
            conn = psycopg2.connect(database="trading",
                                    user="postgres",
                                    password="postgres")
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            execute_batch(cursor, insert_stmt, df.values)
            conn.commit()
            cursor.close()


def get_yahoo_fin_urls():
    NO_URL_COUNT=1500
    yahoo = "https://in.finance.yahoo.com"
    i = 0
    url_list = []
    while i < NO_URL_COUNT:
        url = "https://in.finance.yahoo.com/most-active?offset=" + str(i) + "&count=25"
        url_list.append(url)
        i += 25
    urls = []
    for url in url_list:
        try:
            bs = h.parse_url(url)
            if bs:
                std_data = bs.find('div', {'class': "Ovx(s)"}).find_all("table", {"class": "W(100%)"})[0].find_all('tr')
                for tr in std_data:
                    link = tr.find('a', href=True, text=True)['href']
                    url = (yahoo + link).replace('?p', '/history?p')
                    if url not in urls:
                        urls.append(url)
        except Exception as err:
            print("Exception = ", str(err))
    print("No of URLs = ", len(urls))
    return urls



def parse_yahoo_stk_hist(url):

    try:
        name = url.split("=")[1].split(".")[0]
        # print("Name = ", name)
        bs = h.parse_url(url)
        if bs:
            table = bs.find('div', {'class': "Pb(10px) Ovx(a) W(100%)"}).find_all("table", {"class": "W(100%) M(0)"})[0]
            data = [
                [td.string.strip() for td in tr.find_all('td') if td.string]
                for tr in table.find_all('tr')[2:]
            ][:-1]
            # print(data)
            # data.insert(0, ['STK_DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'ACLOSE', 'VOLUME'])
            # Set pandas options
            pd.set_option('display.max_columns', None)
            pd.set_option('display.expand_frame_repr', False)
            pd.set_option('max_colwidth', 0)
            df = pd.DataFrame(data, columns=['STK_DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'ACLOSE', 'VOLUME'])
            df = df.assign(NSE_CODE=Series(name, index=df.index))
            df = df.drop(columns='ACLOSE')
            cols = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
            df[cols] = df[cols].replace({'\$': '', ',': ''}, regex=True)
            # Drop a row by condition
            df = df[df['OPEN'].notnull()]
            drop_cols = ['STK_DATE', 'NSE_CODE']
            cols = df.columns.drop(drop_cols)
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            df = df.fillna(0)

            if len(df) > 0:
                df_columns = list(df)
                table = "STK_INFO_HISTORY"
                constraint = ', '.join(['STK_DATE', 'NSE_CODE'])
                values = "to_date(%s, 'DD-MON-YYYY'), %s, %s, %s, %s, %s, %s"
                insert_stmt = create_update_query(table, df_columns, values, constraint)
                conn = psycopg2.connect(database="trading",
                                        user="postgres",
                                        password="postgres")
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = conn.cursor()
                execute_batch(cursor, insert_stmt, df.values)
                conn.commit()
                cursor.close()
    except Exception as err:
        traceback.print_exc()
        print("Exception = ", str(err))


def create_update_query(table, df_columns, values, constraint):
    """This function creates an upsert query which replaces existing data based on primary key conflicts"""
    columns = ",".join(df_columns)
    updates = ', '.join([f'{col} = EXCLUDED.{col}' for col in df_columns])
    query = f"""INSERT INTO {table} ({columns}) 
                VALUES ({values}) 
                ON CONFLICT ({constraint}) 
                DO UPDATE SET {updates};"""
    query.split()
    query = ' '.join(query.split())
    print(query)
    return query


if __name__ == "__main__":
    # create()
    st_time = time.time()
    urls = get_yahoo_fin_urls()
    print("Execution time after getting urls = {0:.5f}".format(time.time() - st_time))
    #url = "https://in.finance.yahoo.com/quote/3IINFOTECH.NS/history?p=3IINFOTECH.NS"
    for url in urls:
        parse_yahoo_stk_hist(url)
    print("Execution time = {0:.5f}".format(time.time() - st_time))


    # cat_up = category.upper()*-m / nhb-
    # print("CATEGORY = {} and count = {}".format(cat_up, len(final_data[category])))
    # df = pd.DataFrame(final_data[category])
    # df = df.set_index("NAME")
    # # Slice it as needed
    # sliced_df = df.loc[:, ['MARKET CAP (Rs Cr)', 'EPS (TTM)', 'P/E', 'INDUSTRY P/E', 'BOOK VALUE (Rs)',
    #                        'FACE VALUE (Rs)', 'DIV YIELD.(%)']]
    # filtered_df = sliced_df[sliced_df['EPS (TTM)'] != '-']
    # filtered_df = filtered_df.apply(pd.to_numeric, errors='ignore')
    # sorted_df = filtered_df.sort_values(by=['EPS (TTM)', 'P/E'], ascending=[False, False])
    # writer_orig = pd.ExcelWriter(os.path.join(commons.get_prop('base-path', 'output'), cat_up + '_Listings.xlsx'),
    #                              engine='xlsxwriter')
    # sorted_df.to_excel(writer_orig, index=True, sheet_name='report')
    # writer_orig.save()
