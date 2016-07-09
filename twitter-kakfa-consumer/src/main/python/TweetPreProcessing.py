import re

class TweeTPreProcessing:
    'Common base class for all employees'
    # empCount = 0

    # def __init__(self):

    # def displayCount(self):
    #     print "Total Employee %d" % Employee.empCount

    # def displayEmployee(self):
    #     print "Name : ", self.name, ", Salary: ", self.salary

    def stemming(self, text):
        text = text.lower().strip()
        text = re.sub("[^0-9a-zA-Z ]", '', text)
        return text