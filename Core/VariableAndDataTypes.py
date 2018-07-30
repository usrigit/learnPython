
"""
Python is Interpreted
Many languages are compiled, meaning the source code you create needs to be translated into machine code,
the language of your computer’s processor, before it can be run.

Programs written in an interpreted language are passed straight to an interpreter that runs them directly.

This makes for a quicker development cycle because you just type in your code and run it,
without the intermediate compilation step. One potential downside to interpreted languages is execution speed.

Programs that are compiled into the native language of the computer processor
tend to run more quickly than interpreted programs.
For some applications that are particularly computationally intensive,
like graphics processing or intense number crunching, this can be limiting.

In practice, however, for most programs, the difference in execution speed is measured in milliseconds,
or seconds at most, and not appreciably noticeable to a human user.
The expediency of coding in an interpreted language is typically worth it for most applications.
"""

# variable assignment
# Python variables do not need explicit declaration to reserve memory space.
# The declaration happens automatically when you assign a value to a variable

s = 10
print("var s = ",s)
print("Type of variable is ", type(s))

# Data types
# List, String, Dictionary, Tuple, Set, Numeric
# Numeric -> Int, Float and Complex -> these are immutable

A = 20
print("A= and type is ", (A, type(A)))

A = 20.15
print("A= and type is ", (A, type(A)))

# List -> You can consider the Lists as Arrays in C, but in List you can store elements of different types,
# but in Array all the elements should of the same type.

Subjects = ['Physics', 'Chemistry', 'Maths', 2]
print("Subjects = ", Subjects)
print("Subjects [0] = ", Subjects[0])
print("Subjects [0:2]", Subjects[0:2])
print("Subjects [3]", Subjects[3])
del Subjects[2]
print("Subjects = ", Subjects)
print("len (Subjects) = ", len(Subjects))
print("Subjects * 2", (Subjects * 2))
print("Subjects [::-1]", Subjects [::-1])

# A Tuple is a sequence of immutable Python objects. Tuples are sequences, just like Lists.
# The differences between tuples and lists are: Tuples cannot be changed unlike lists
# Tuples use parentheses, whereas lists use square brackets
# Tuples are faster than Lists. If you’re defining a constant set of values which you just want to iterate,
# then use Tuple instead of a List.

Chelsea = ('Hazard', 'Lampard', 'Terry')

# Strings: Strings are amongst the most popular data types in Python
String_Name = "Welcome To edureka!"

print(len(String_Name)) # String Length
print(String_Name.index('e')) # Locate a character in String
print(String_Name.count('k')) # Count the number of times a character is repeated in a String
print(String_Name[3:6]) # Slicing
print(String_Name[::-1]) # Reverse a String
print(String_Name.upper()) # Convert the letters in a String to upper-case
print(String_Name.lower())


# Set: A Set is an unordered collection of items. Every element is unique.
# A Set is created by placing all the items (elements) inside curly braces {}, separated by comma.

Set_1 = {1, 2, 3}

# Union: Union of A and B is a set of all the elements from both sets. Union is performed using | operator. Consider the below example:

A = {1, 2, 3, 4}
B = {3, 4, 5, 6}
print ( A | B)

# dictionary (key - value)
Dict = {'Name' : 'Saurabh', 'Age' : 23}
print(Dict['Name'])

# update and add new element in dictionary
Dict['Age'] = 32
Dict['Address'] = 'Starc Tower'
print("Dictionary is ", Dict)

# Operators in Python[Arthmetic, Assignment, Bitwise,
# Comparision, Identity, Membership and Logical]

a = 21
b = 10
c = 0

c = a + b
print(c)

c = a - b
print(c)

c = a * b
print(c)

c = a / b
print(c)

c = a % b
print(c)
a = 2
b = 3
c = a ** b
print(c)

if (a == b):
    print("a is equal to b")
else:
    print("a is not equal to b")

if (a != b):
    print("a is not equal to b")
else:
    print("a is equal to b")

if (a < b):
    print("a is less than b")
else:
    print("a is not less than b")

if (a > b):
    print("a is greater than b")
else:
    print("a is not greater than b")

a = 5
b = 20
if (a <= b):
    print("a is either less than or equal to b")
else:
    print("a is neither less than nor equal to b")

if (a >= b):
    print("a is either greater than  or equal to b")
else:
    print("a is neither greater than  nor equal to b")

a = 58        # 111010
b = 13        # 1101
c = 0

c = a + b
print(c)

c += a
print(c)

c *= a
print(c)

c /= a
print(c)

c = 2
c %= a
print(c)

c **= a
print(c)

a = 58        # 111010
b = 13        # 1101
c = 0

c = a & b
print("& c val = ", c)  # 8 = 1000

c = a | b
print(c)  # 63 = 111111

c = a ^ b
print(c)  # 55 = 110111

c = a << 2
print(c)  # 232 = 11101000

c = a >> 2
print(c)  # 14 = 1110

x = True
y = False

print('x and y is', x and y)
print('x or y is', x or y)
print('not x is', not x)

X = [1, 2, 3, 4]
A = 3
print(A in X)
print(A not in X)

X1 = 'Welcome To edureka!'
X2 = 1234
Y1 = 'Welcome To edureka!'
Y2 = 1234

print(X1 is Y1)
print(X1 is not Y1)
print(X1 is not Y2)
print(X1 is X2)

# While loop
count = 0
while (count < 10):
    print(count)
    count = count + 1

print("Good bye!")

# For loop
fruits = ['Banana', 'Apple', 'Grapes']

for index in range(len(fruits)):
    print(fruits[index])

count = 1
for i in range(10):
    print(str(i) * i)

    for j in range(0, i):
        count = count + 1


def add (a, b):
    return a + b


c = add(10,20)
print(c)
