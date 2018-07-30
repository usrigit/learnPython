"""
Types of Inheritances
1. Single
2. Multi level
3. Hierarchical
4. Multiple

"""


class Employee:
    num_employee = 0
    raise_amount = 1.04

    def __init__(self, first, last, sal):
        print("Employee base class")
        self.first = first
        self.last = last
        self.sal = sal
        self.email = first + '.' + last + '@company.com'
        Employee.num_employee += 1

    def fullname(self):
        return '{} {}'.format(self.first, self.last)

    def apply_raise(self):
        self.sal = int(self.sal * Employee.raise_amount)


class Developer(Employee):
    raise_amount = 1.10

    def __init__(self, first, last, sal, prog_lang):
        print("Am from developer class")
        super().__init__(first, last, sal)
        self.prog_lang = prog_lang


emp_1 = Developer('aayushi', 'johari', 1000000, 'python')
print(emp_1.prog_lang)


# Python: Polymorphism

class Animal:
    def __init__(self, name):
        self.name = name
        print("Animal name", self.name)

    def talk(self):
        pass


class Dog(Animal):

    def talk(self):
        print('Woof', self.name)


class Cat(Animal):

    def talk(self):
        print('MEOW!')


c = Cat('kitty')
c.talk()

d = Dog("Boomer")
d.talk()


