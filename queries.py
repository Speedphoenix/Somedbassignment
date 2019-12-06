import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient['company']
employees = mydb['employees']

print("Query 01")
# The highest salary of clerks

# the next() method goes to the next element of an iterator or iterable.
employees.find({"job": "clerk"}, { "salary": 1, "_id": 0 }).sort("salary", -1).next()


print("Query 02")
# The total salary of managers


print("Query 03")
# The lowest, average and highest salary of the employees
pipeline3 = [
    {
        "$group": {
            "_id": None,
            "lowest": {"$min": "$salary"},
            "average": {"$avg": "$salary"},
            "highest": {"$max": "$salary" }
        }
    },
    {
        "$project": {
            "_id": 0
        }
    }
]
employees.aggregate(pipeline3).next()


print("Query 04")
# The name of the departments


print("Query 05")
# For each job: the job and the average salary for that job
pipeline5 = [
  {
    "$group": {
      "_id": "$job",
      "averageSalary": { "$avg": "$salary" }
    }
  }
]
for el in employees.aggregate(pipeline5):
    print(el)


print("Query 06")
# For each department: its name, the number of employees and the average salary in that department (null departments excluded)


print("Query 07")
# The highest of the per-department average salary (null departments excluded)
pipeline7 = [
  {
    "$group": {
      "_id": "$department",
      "averageSalary": { "$avg": "$salary" }
    }
  },
  {
    "$group": {
      "_id": None,
      "highestAvgSalary": { "$max": "$averageSalary" }
    }
  },
  {
    "$project": { "_id": 0 }
  }
]
employees.aggregate(pipeline7).next()


print("Query 08")
# The name of the departments with at least 5 employees (null departments excluded)

print("Query 09")
# The cities where at least 2 missions took place

print("Query 10")
# The highest salary

print("Query 11")
# The name of the departments with the highest average salary

print("Query 12")
# For each city in which a mission took place, its name (output field "city") and the number of missions in that city

print("Query 13")
# The name of the departments with at most 5 employees

print("Query 14")
# The average salary of analysts

print("Query 15")
# The lowest of the per-job average salary

print("Query 16")
# For each department: its name and the highest salary in that department

print("Query 17")
# The number of employees

print("Query 18")
# One of the employees, with pretty printing (2 methods)

print("Query 19")
# All the information about employees, except their salary, commission and missions

print("Query 20")
# The name and salary of all the employees (without the field _id)
