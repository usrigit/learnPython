import commons, os
from commons import Helper as h
from commons import Constants as c
from pandas import Series
from psycopg2.extras import execute_batch
import pandas as pd
from dao import PostgreSQLCon as db

STK_RATIO_ELEMENTS = {
    'STK_YEAR': [],
    'EPS': [],
    'BOOK_VAL': [],
    'DIVIDEND': [0.0, 0.0, 0.0, 0.0, 0.0],
    'RET_ON_EQ': [0.0, 0.0, 0.0, 0.0, 0.0],
    'PRC_TO_BOOK': [0.0, 0.0, 0.0, 0.0, 0.0],
    'PRC_TO_SALE': [0.0, 0.0, 0.0, 0.0, 0.0],
    'CURR_RATIO': [0.0, 0.0, 0.0, 0.0, 0.0],
    'DEBT_EQUITY': [0.0, 0.0, 0.0, 0.0, 0.0]
}


def get_ratios_from_mc(url):
    # url = "https://www.moneycontrol.com/financials/abhinavleasingfinance/ratiosVI/ALF03#ALF03"
    print("Processing URL = ", url)
    bs = h.parse_url(url)
    data_frame = None
    if bs:
        std_data = bs.find('div', {'class': 'PB10'}).find('div', {'class': 'FL gry10'})
        if std_data:
            nse_code = (std_data.text.split("|")[1]).split(":")[1].strip()
            nse_code = nse_code if nse_code else 'N/A'
            data = [
                [
                    [td.string.strip() for td in tr.find_all('td') if td.string]
                    for tr in table.find_all('tr')[2:]
                ]
                for table in bs.find_all("table", {"class": "table4"})[2:]
            ]

            if data and len(data) > 0:
                ele_list = data[0]
                STK_RATIO_ELEMENTS['STK_YEAR'] = data[0][0]
                i = 2
                while i < len(ele_list) - 4:
                    arr = ele_list[i]
                    if len(arr) > 5:
                        key = c.STK_RATIO_CON.get(arr[0])
                        val = arr[1:]
                        if key: STK_RATIO_ELEMENTS[key] = val
                    i += 1
                print("STK RATIO = ", STK_RATIO_ELEMENTS)
                data_frame = get_data_frame(nse_code, STK_RATIO_ELEMENTS)
                print(data_frame)

    return data_frame


def get_data_frame(nse_code, data):
    # Set pandas options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', 0)
    df = pd.DataFrame(data)
    df = df.assign(NSE_CODE=Series(nse_code, index=df.index))
    cols = df.columns.drop(['STK_YEAR', 'NSE_CODE'])
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    df = df.fillna(0)

    return df


def load_stk_ratio():
    df_frames = []
    file_path = os.path.join(commons.get_prop('base-path', 'ratio-input'))
    files = [os.path.join(file_path, fn) for fn in next(os.walk(file_path))[2]]
    try:
        for file in files:
            read_lines = h.read_list_from_json_file(file)
            print("No of urls to process", len(read_lines))
            for line in read_lines:
                df = get_ratios_from_mc(line)
                if df is not None and isinstance(df, pd.DataFrame) \
                        and not df.empty:
                    df_frames.append(df)
    except Exception as err:
        print(str(err))
    print("Data frames size = ", len(df_frames))
    try:
        result = pd.concat(df_frames, ignore_index=True)
        if len(result) > 0:
            df_columns = list(result)
            table = "STK_PERF_HISTORY"
            columns = ",".join(df_columns)
            values = "(to_date(%s, 'MONYY'), %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) VALUES {}".format(table, columns, values)
            print("Count of rows insert into DB = ", len(result.values))
            curr, con = db.get_connection()
            execute_batch(curr, insert_stmt, result.values)
            con.commit()
            db.close_connection(con, curr)

    except Exception as err:
        print("Exception while inserting data into table ", str(err))


if __name__ == "__main__":
    load_stk_ratio()
    #  get_ratios_from_mc("url")
