
URL = 'https://www.moneycontrol.com/india/stockpricequote'
THREAD_COUNT = 50
INFO = "INFO"
STK_HIST_LOAD_DATA = "INSERT INTO STK_PERF_HISTORY (NSE_CODE, EPS, BOOK_VAL, DIVIDEND, RET_ON_EQ, " \
        "PRC_TO_BOOK, PRC_TO_SALE, CURR_RATIO, DEBT_EQUITY, STK_YEAR) " \
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
