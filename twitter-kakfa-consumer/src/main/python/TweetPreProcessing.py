import re

class TweetPreProcessing:
    'Common base class for all employees'
    # empCount = 0

    def __init__(self,x):
        self.x = x

    # def displayCount(self):
    #     print "Total Employee %d" % Employee.empCount

    # def displayEmployee(self):
    #     print "Name : ", self.name, ", Salary: ", self.salary

    def stemming2(self, text):
        text = text.lower().strip()
        text = re.sub("[^0-9a-zA-Z ]", '', text)
        return text