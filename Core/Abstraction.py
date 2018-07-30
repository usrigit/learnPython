from abc import ABC, abstractmethod


class Employee(ABC):

    @abstractmethod
    def calculate_salary(self, sal):
        print("Employee Sal", sal)


class Developer(Employee):

    def calculate_salary(self, sal):
        final_salary = sal * 1.10
        print("Developer Sal", final_salary)
        return final_salary


class Manager(Employee):

    def calculate_salary(self, sal):
        final_salary = sal * 1.30
        print("Manager Sal", final_salary)
        return final_salary

    def emp_name(self, name):
        print("Manager name is", name)


mgr = Manager()
print(mgr.emp_name("Rama"))
print(mgr.calculate_salary(10000))

emp_1 = Developer()
print(emp_1.calculate_salary(10000))