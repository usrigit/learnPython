
import os
file_path = "D:\Sri\Daily_Status.txt"


def file_read(input):
    if os.path.exists(input) and os.path.isfile(input):
        file = open(input, "r")
        # Read file in chunks file.read(size) if no size then complete file
        # Read file in lines file.readlines()
        for line in file:
            print(line)


file_read(file_path)


def file_write():
    with open("D:/Writing_Into_File.txt", "w") as f:
        f.write("First Line\n")
        f.write("Second Line\n")
    f.close()

file_write()


