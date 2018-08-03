# NumPy is a Python package which stands for ‘Numerical Python’.
# It is the core library for scientific computing, which contains a powerful n-dimensional array object

import numpy as np
import time
import sys

# Single dimensional array
a = np.array([1, 2, 3])
print(a)

# Multi dimensional array
a = np.array([(1, 2, 3), (4, 5, 6)])
print(a)

SIZE = 1000

L1 = range(SIZE)
L2 = range(SIZE)
A1 = np.arange(SIZE)
A2 = np.arange(SIZE)

print("A1 = ", A1)
print("A2 = ", A2)

start = time.time()
result = [(x, y) for x, y in zip(L1, L2)]
print("result = ", result)
print((time.time() - start) * 1000)

start = time.time()
result = A1 + A2
print((time.time() - start) * 1000)

a = np.array([(1, 2, 3, 4), (5, 6, 7, 8)])
print("dimension = ", a.ndim)  # Get dimension details
print("Data type - ", a.dtype)  # type of variables
print("Size in bytes = ", a.itemsize)  # size of the array in bytes
print("Shape of array", a.shape)
print("Reshape of array", a.reshape(4, 2))
a = a.reshape(4, 2)
print("Slice of array", a[0:, 1])  # colon represents all the rows, including zero

a = np.linspace(1, 10, 5)
print("even space cut by interval (10)", a)

a = np.array([1, 100, 10])
print("Min = ", np.min(a))
print("SUM = ", np.sum(a))
print("sort=", np.sort(a))

a = np.array([(1, 2, 3), (3, 4, 5)])
print("shape of the array = ", a.shape)
print(a.sum(axis=0))  # 0 indicates column and 1 indicates rows


a=np.array([(1,2,3),(3,4,5,)])
print("SQRT=", np.sqrt(a))
print("STANDARD DEVIATION=", np.std(a))

x= np.array([(1,2,3),(3,4,5)])
y= np.array([(1,2,3),(3,4,5)])
print("Addition = ", x+y)
print("SUB=", x-y)
print("MUL=", x*y)
print("DIV=", x/y)
print("VERTICAL STK=", np.vstack((x,y)))
print("HORIZONTAL STK=", np.hstack((x,y)))
print("RAVEL=", x.ravel())
