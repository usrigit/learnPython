def parent():
    try:
        print("I am calling from parent function")
        child()
    except Exception as e:
        print("Error is from parent ", str(e))


def child():
    try:
        print("I am calling from child")
        d = 5 / 0
        print("d val = ", d)
    except ZeroDivisionError as err:
        print("From child = ", err)
        raise err


def ini_stk_ratio_dic(ratio_dict, size):

    ratio_dict['EPS'] = [0.0] * size
    ratio_dict['BOOK_VAL'] = [0.0] * size
    ratio_dict['DIVIDEND'] = [0.0] * size
    ratio_dict['RET_ON_EQ'] = [0.0] * size
    ratio_dict['PRC_TO_BOOK'] = [0.0] * size
    ratio_dict['PRC_TO_SALE'] = [0.0] * size
    ratio_dict['CURR_RATIO'] = [0.0] * size
    ratio_dict['DEBT_EQUITY'] = [0.0] * size
    print("dictionary = ", ratio_dict)


if __name__ == "__main__":
    ini_stk_ratio_dic({}, 4)