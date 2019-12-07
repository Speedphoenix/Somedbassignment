import pymongo
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient['company']
employees = mydb['employees']

print("Query 01")
# The highest salary of clerks

# the next() method goes to the next element of an iterator or iterable.
print(employees.find({"job": "clerk"}, { "salary": 1, "_id": 0 }).sort("salary", -1).next())


print("Query 02")
# The total salary of managers
print(employees.aggregate([
    {
        "$match":
        {
            "job": "manager"
        }
    },
    {
        "$group": {
            "_id": "$job",
            "sumSalary": { "$sum": "$salary" }
        }
    }
]).next())


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
print(employees.aggregate(pipeline3).next())


print("Query 04")
# The name of the departments
pipeline4 = [
    {
        # to exclude null elements
        "$match": {
            "department": {
                "$exists": True,
                "$ne": None
            }
        }
    },
    {
        "$group": {
            "_id": "$department"
        }
    },
    {
        "$project": {
            "_id": 0,
            "name": "$_id.name"
        }
    }
]
for el in employees.aggregate(pipeline4):
    print(el)


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
pipeline6 = [
  {
    # to exclude null elements
    "$match": {
      "department": {
        "$exists": True,
        "$ne": None
      }
    }
  },
  {
    "$group": {
      "_id": "$department.name",
      "averageSalary": { "$avg": "$salary" },
      "number_of_employees": { "$sum": 1 }
    }
  }
]
for el in employees.aggregate(pipeline6):
    print(el)


print("Query 07")
# The highest of the per-department average salary (null departments excluded)
pipeline7 = [
    {
        "$match": {
            "department": {
                "$exists": True,
                "$ne": None
            }
        }
    },
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
print(employees.aggregate(pipeline7).next())


print("Query 08")
# The name of the departments with at least 5 employees (null departments excluded)
pipeline8 = [
    {
        "$match": {
            "department": {
                "$exists": True,
                "$ne": None
            }
        }
    },
    {
        "$group": {
            "_id": "$department",
            "number_of_employees": {
                 "$sum": 1
             }
        }
    },
    {
        "$match":{
            "number_of_employees" : { "$gte": 5 }
        }
    },
    {
        "$project": {
            "_id": 0,
            "name": "$_id.name"
        }
    }
]
for el in employees.aggregate(pipeline8):
    print(el)


print("Query 09")
# The cities where at least 2 missions took place
pipeline9 = [
    {
        "$project": {
            "_id": 0,
            "missions": 1
        }
    },
    {
        "$unwind": "$missions"
    },
    {
        "$group": {
            "_id": "$missions.location",
            "sum": { "$sum": 1 }
        }
    },
    {
        "$match": {
            "sum": { "$gte": 2 }
        }
    },
    {
        "$project": { "_id": 1 }
    }
]
for el in employees.aggregate(pipeline9):
    print(el)


print("Query 10")
# The highest salary
print(employees.aggregate([
    {
        "$group": {
            "_id": None,
            "maxSalary": { "$max": "$salary" }
        }
    },
    {
        "$project": {
            "_id": 0
        }
    }
]).next())


print("Query 11")
# The name of the departments with the highest average salary
pipeline11 = [
    {
        "$match": {
            "department": {
                "$exists": True,
                "$ne": None
            }
        }
    },
    {
        "$group": {
            "_id": "$department",
            "averageSalary": {
                "$avg": "$salary"
            }
        }
    },
    {
        "$sort": { "averageSalary": -1 }
    },
    {
        "$limit": 1
    },
    {
        "$project": {
          "_id": 0,
          "name": "$_id.name"
        }
    }
]
print(employees.aggregate(pipeline11).next())


print("Query 12")
# For each city in which a mission took place, its name (output field "city") and the number of missions in that city
pipeline12 = [
    {
        "$project": {
            "_id": 0,
            "missions": 1
        }
    },
    {
     "$unwind": "$missions"
    },
    {
        "$group": {
            "_id": "$missions.location",
            "sum": { "$sum": 1 }
        }
    },
    {
        "$project": {
            "_id": 0,
            "city": "$_id",
            "nbmissions": "$sum",
        }
    }
]
for el in employees.aggregate(pipeline12):
    print(el)

print("Query 13")
# The name of the departments with at most 5 employees
pipeline13 = [
    # {
    #     "$match": {
    #         "department": {
    #             "$exists": True,
    #             "$ne": None
    #         }
    #     }
    # },
    {
        "$group": {
            "_id": "$department",
            "nbEmployees": {
                "$sum": 1
            }
        }
    },
    {
        "$match": {
            "nbEmployees": { "$lte": 5 }
        }
    },
    {
        "$project": {
            "_id": 0,
            "name": "$_id.name"
        }
    }
]

for el in employees.aggregate(pipeline13):
    print(el)


print("Query 14")
# The average salary of analysts
print(employees.aggregate([
    {
        "$match": {
            "job" : "analyst"
        }
    },
    {
        "$group": {
            "_id": None,
            "avgSalary": { "$avg": "$salary" }
        }
    },
    {
        "$project": {
            "_id": 0
        }
    }
]).next())


print("Query 15")
# The lowest of the per-job average salary
pipeline15 = [
    {
        "$group": {
            "_id": "$job",
            "averageSalary": {
                "$avg": "$salary"
            }
        }
    },
    {
        "$sort": { "averageSalary": 1 }
    },
    {
        "$limit": 1
    },
    {
        "$project": {
            "_id": 0,
            "lowestAvgSalary": "$averageSalary"
        }
    }
]
for el in employees.aggregate(pipeline15):
    print(el)

print("Query 16")
# For each department: its name and the highest salary in that department
pipeline16 = [
    # {
    #     "$match": {
    #         "department": {
    #             "$exists": True,
    #             "$ne": None
    #         }
    #     }
    # },
    {
        "$group": {
            "_id": "$department",
            "highestSalary": {
                "$max": "$salary",
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "name": "$_id.name",
            "highestSalary": 1
        }
    }
]
for el in employees.aggregate(pipeline16):
    print(el)


print("Query 17")
# The number of employees
print(employees.aggregate([
    {
        "$group": {
            "_id": None,
            "number_of_employees": { "$sum": 1 }
        }
    },
    {
        "$project": {
            "_id": 0
        }
    }
]).next())


print("Query 18")
# One of the employees, with pretty printing (2 methods)
print(employees.find_one())
print(employees.find().next())


print("Query 19")
# All the information about employees, except their salary, commission and missions
for el in employees.find({}, { "salary": 0, "commission": 0, "missions": 0 }):
      print(el)


print("Query 20")
# The name and salary of all the employees (without the field _id)
for el in employees.find({}, { "_id": 0, "name": 1, "salary": 1 }):
      print(el)
