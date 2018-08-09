import threading
from queue import Queue
import time
import shutil

print_lock = threading.Lock()
print("print lock value =", print_lock)


def copy_op(file_data):
    with print_lock:
        print("Starting thread : {}".format(threading.current_thread().name))

    mydata = threading.local()
    print("mydata value =", mydata)
    mydata.ip, mydata.op = next(iter(file_data.items()))

    shutil.copy(mydata.ip, mydata.op)

    with print_lock:
        print("Finished thread : {}".format(threading.current_thread().name))


def process_queue():
    print("print lock in pq =", print_lock)
    while True:
        file_data = compress_queue.get()
        print("file_data in Queue = ", file_data)
        copy_op(file_data)
        compress_queue.task_done()
        print("task done in queue =", print_lock)
    print("print lock value in pq =", print_lock)


compress_queue = Queue()

output_names = [{'D:\\1_AWS\\AWS_Exam_prep.txt': 'D:\\1_AWS\\AWS_Exam_prep_1.txt'},
                {'D:\\1_AWS\\aws_lamda_cd.txt': 'D:\\1_AWS\\aws_lamda_cd1.txt'}]

for i in range(1):
    t = threading.Thread(target=process_queue)
    t.daemon = True
    print("Thread %s status before start %s= ", (t.name, t.isAlive()))
    t.start()
    print("Thread %s status after start %s= ", (t.name, t.isAlive()))

start = time.time()

for file_data in output_names:
    compress_queue.put(file_data)
    print("Queue size = ", compress_queue.qsize())

compress_queue.join()

print("Execution time = {0:.5f}".format(time.time() - start))
