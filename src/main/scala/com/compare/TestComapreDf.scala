package com.compare

import com.compare.DfCompare.sc

class TestComapreDf {

  var input = sc.sqlContext.createDataFrame(Seq(
    ("sumeet", 101, 23, "Maths", "80", "Dancing", "Mumbai"),
    ("sumeet", 101, 23, "Sociology", "90", "Dancing", "Mumbai"),
    ("sumeet", 101, 23, "English", "85", "Dancing", "Mumbai"),
    ("Sourav", 102, 24, "Biology", "45", "Dancing", "Mumbai"),
    ("Sourav", 102, 24, "Astronomy", "45", "Dancing", "Mumbai"),
    ("Sourav", 102, 24, "Science", "45", "Dancing", "Mumbai"),
    ("Ajay", 103, 26, "Chemistry", "45", "Dancing", "Mumbai"),
    ("Ajay", 103, 26, "Physics", "45", "Dancing", "Mumbai"),
    ("Ajay", 103, 26, "Electrical", "45", "Dancing", "Mumbai"),
    ("Payal", 104, 29, "Electronics", "45", "Dancing", "Mumbai"),
    ("Payal", 104, 29, "English", "45", "Dancing", "Mumbai"),
    ("Payal", 1041, 29, "Sociology", "45", "Dancing", "Mumbai"),
    ("Pulkit", 105, 25, "English", "45", "Dancing", "Mumbai"),
    ("Pulkit", 105, 25, "Software", "45", "Dancing", "Mumbai"),
    ("Pulkit", 105, 25, "Electrical", "45", "Dancing", "Mumbai")
  )).toDF("name", "Id", "Age", "Subject", "Marks", "Hobby", "City")

  var target = sc.sqlContext.createDataFrame(Seq(
    ("sumeet", 101, 23, "Maths", "80", "Dancing", "Mumbai"),
    ("sumeet", 101, 23, "Sociology", "90", "Music", "Mumbai"),
    ("sumeet", 101, 23, "English", "85", "Dancing", "Mumbai"),
    ("Sourav", 102, 24, "Astronomy", "45", "Wrestling", "Mumbai"),
    ("Sourav", 102, 24, "Science", "45", "Dancing", "Mumbai"),
    ("Ajay", 103, 26, "Chemistry", "45", "Dancing", "Mumbai"),
    ("Ajay", 103, 26, "Physics", "45", "Dancing", "Mumbai"),
    ("Ajay", 103, 26, "Electrical", "45", "Dancing", "Mumbai"),
    ("Payal", 104, 29, "Electronics", "45", "Dancing", "Mumbai"),
    ("Payal", 1041, 29, "Sociology", "45", "Singing", "Mumbai"),
    ("Pulkit", 105, 25, "English", "45", "Dancing", "Mumbai"),
    ("Pulkit", 105, 25, "Software", "45", "Dancing", "Mumbai"),
    ("Pulkit", 105, 25, "Electrical", "45", "Dancing", "Mumbai"),
    ("Vikram", 104, 24, "English", "45", "Dancing", "Mumbai"),
    ("Vikram", 104, 25, "English", "45", "Dancing", "Mumbai"),
    ("Vikram", 104, 26, "English", "45", "Dancing", "Mumbai"),
    ("Rohit", 104, 27, "English", "45", "Dancing", "Mumbai"),
    ("Rohit", 104, 28, "English", "45", "Dancing", "Mumbai"),
    ("Rohit", 104, 29, "English", "45", "Dancing", "Mumbai")
  )).toDF("name", "Id", "Age", "Subject", "Marks", "Hobby", "City")
}