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


if __name__ == "__main__":
    parent()