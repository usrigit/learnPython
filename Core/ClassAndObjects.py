
class Employee:
    lac = 100000 # Class variable

    def __init__(self, emp_id, emp_name, emp_sal):
        # Instance variables
        self.emp_id = emp_id
        self.emp_name = emp_name
        self.emp_sal = emp_sal

    def full_name(self):
        return "Upputuri " + self.emp_name

    def get_sal(self):
        return self.emp_sal * self.lac


emp = Employee("12", "Srikanth", 100)
print(emp.emp_name)
print(emp.full_name())
print(emp.get_sal())
print(emp.__dict__) # Builtin class attribute
femp = Employee("11", "Deepu", 200)
print(femp.emp_name)
print(femp.full_name())
print(femp.get_sal())