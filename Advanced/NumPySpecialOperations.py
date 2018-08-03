import numpy as np
import matplotlib.pyplot as plt

a= np.array([1,2,3])
print("Exponential = ", np.exp(a))
print("log value = ", np.log(a))
print("log base 10 value= ", np.log10(a))

x= np.arange(0,3*np.pi,0.5)
print("PI = ", np.pi)
print("X = ", x)
y=np.sin(x)
print("Y=", y)
plt.plot(x,y)
plt.show()

y=np.tan(x)
plt.plot(x,y)
plt.show()