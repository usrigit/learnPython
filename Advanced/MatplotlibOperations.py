from matplotlib import pyplot as plt
from matplotlib import style as s

# s.use("ggplot")
#
# X = [10, 20, 30]
# Y = [100, 200, 100]
#
# X2 = [20, 30, 40]
# Y2 = [40, 20, 10]
#
# plt.plot(X, Y, 'g', label='LineOne', linewidth=5)
# plt.plot(X2, Y2, 'b', label='LineTwo', linewidth=5)
#
# plt.title("Info")
# plt.xlabel("X-axis")
# plt.ylabel("Y-axis")
#
# plt.legend()
# plt.grid(True, color='K')
# plt.show("img1")

X = [1, 3, 5, 7, 9]
Y = [5, 2, 7, 8, 2]

X2 = [20, 30, 40]
Y2 = [40, 20, 10]
# Bar graph
plt.bar(X, Y, color = 'g', label='LineOne')
plt.bar(X2, Y2, color = 'b', label='LineTwo')
plt.legend()

plt.xlabel("bar number")
plt.ylabel("bar height")

plt.title("Bar graph")

plt.show("img1")






