db = db.getSiblingDB("company");
// now do stuff with db.employees

// to run the files in the mongo shell use
// load("queries.js");

print("Query 01");
// The highest salary of clerks

db.employees.find({ "job": "clerk" }, { "salary": 1, "_id": 0 }).sort({salary:-1}).limit(1);

// or, to get the whole employee instead of just the salary
// db.employees.find({ "job": "clerk" }).sort({salary:-1}).limit(1)


print("Query 02")
// The total salary of managers
/// POSSIBLE

db.employees.aggregate([
  {
    $match :
    {
      job : "manager"
    }
  },
  {
    $group: {
      _id: "$job",
      sumSalary: { $sum: "$salary" }
    }
  }
]);

print("Query 03")
// The lowest, average and highest salary of the employees
db.employees.aggregate([
  {
    $group : {
      _id: null,
      lowest: {$min: "$salary"},
      average: {$avg: "$salary"},
      highest: {$max: "$salary"}
    }
  },
  {
    $project: {_id: 0}
  }
]);


print("Query 04")
// The name of the departments
//POSSIBLE

db.employees.aggregate([
  {
    // to exclude null elements
    $match: {
      department: {
        $exists: true,
        $ne: null
      }
    }
  },
  {
    $group: {
      _id: "$department.name"
    }
  }
]);

print("Query 05")
// For each job: the job and the average salary for that job
db.employees.aggregate([
  {
    $group: {
      _id: "$job",
      averageSalary: { $avg: "$salary" }
    }
  }
]);


print("Query 06")
// For each department: its name, the number of employees and the average salary in that department (null departments excluded)
/// in sql :SELECT COUNT(*) AS count, AVG(SAL) AS average, dept.DNAME FROM emp INNER JOIN dept ON emp.DID = dept.DID GROUP BY dept.DNAME
//POSSIBLE
db.employees.aggregate([
  {
    $group: {
      _id: {department : "$name"},
      countEmployees: { $sum:{1} }
    }
  }
]);


print("Query 07")
// The highest of the per-department average salary (null departments excluded)
db.employees.aggregate([
  {
    // to exclude null elements
    $match: {
      department: {
        $exists: true,
        $ne: null
      }
    }
  },
  {
    $group: {
      _id: "$department",
      averageSalary: { $avg: "$salary" }
    }
  },
  {
    $group: {
      _id: null,
      highestAvgSalary: { $max: "$averageSalary" }
    }
  },
  {
    $project: { _id: 0 }
  }
]);


print("Query 08")
// The name of the departments with at least 5 employees (null departments excluded)


print("Query 09")
// The cities where at least 2 missions took place
db.employees.aggregate([
  {
    $project: {
      _id: 0,
      missions: 1
    }
  },
  {
    $unwind: "$missions"
  },
  {
    $group: {
      _id: "$missions.location",
      sum: { $sum: 1 }
    }
  },
  {
    $match: {
      sum: { $gte: 2 }
    }
  },
  {
    $project: { _id: 1 }
  }
]);


print("Query 10")
// The highest salary
//POSSIBLE

db.employees.aggregate([
  {
    $group:
         {
           _id: null,
           maxSalary: { $max: "$salary" }
         }
  },
  {
    $project:{
      _id: 0
    }
  }
]);

print("Query 11")
// The name of the departments with the highest average salary
db.employees.aggregate([
  {
    $match: {
      department: {
        $exists: true,
        $ne: null
      }
    }
  },
  {
    $group: {
      _id: "$department",
      averageSalary: {
        $avg: "$salary"
      }
    }
  },
  {
    $sort: { averageSalary: -1 }
  },
  {
    $limit: 1
  },
  {
    $project: { _id: 1 }
  }
]);


print("Query 12")
// For each city in which a mission took place, its name (output field "city") and the number of missions in that city

print("Query 13")
// The name of the departments with at most 5 employees

print("Query 14")
// The average salary of analysts
//POSSIBLE
db.employees.find({
	"JOB " :  "ANALYST"
},{
	"AVG(SAL)": 1
}
);

print("Query 15")
// The lowest of the per-job average salary

print("Query 16")
// For each department: its name and the highest salary in that department

print("Query 17")
// The number of employees
//POSSIBLE
db.employees.find({

},{
	"COUNT(*)": 1
}
);

print("Query 18")
// One of the employees, with pretty printing (2 methods)

print("Query 19")
// All the information about employees, except their salary, commission and missions

print("Query 20")
// The name and salary of all the employees (without the field _id)
