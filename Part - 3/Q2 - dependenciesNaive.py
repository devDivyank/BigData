"""
file: dependenciesNaive.py
description: CSCI-620.03 - HMWK3 Q2: finding dependencies using naive method
language: python3, SQL
author: Divyank Kulshrestha, dk9924
"""

from itertools import combinations
import psycopg2
from datetime import datetime

def powerset(columnList):
    """
        function that returns the powerset of the columns

        :param columnList: a list of all columns
    """
    combs = []
    n = len(columnList)
    for i in range(0, n + 1):
        for element in combinations(columnList, i):
            combs.append(', '.join(element))
    return combs

# VARIABLES TO SET FOR CONNECTING TO DATABASE
host = "localhost"
dbname = "divyankkulshrestha"
user = "divyankkulshrestha"
password = None
port = 5432

if __name__ == '__main__':
    columns = ["movieid", "type", "startyear", "runtime", "avgrating", "genreid", "genre", "memberid", "birthyear",
               "character"]
    allCombinations = powerset(columns)
    dependencies = []
    print("START TIME: " + str(datetime.now())[11:])
    # connection to database
    with psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port) as connectionToDB:
        cursor = connectionToDB.cursor()
        dependenciesChecked = 0
        # choosing the lhs of dependency to be checked (from the power set)
        for lhs in allCombinations[1:]:
            # choosing the rhs of dependency to be checked (only one column on rhs)
            for rhs in columns:
                # trivial dependency skipped
                if lhs == rhs or (len(lhs.split(',')) > 1 and rhs in lhs):
                    continue
                # using the query written below
                # >>> SELECT lhs, count(DISTINCT rhs) FROM joinedtable GROUP BY lhs HAVING count(DISTINCT rhs) > 1
                cursor.execute("SELECT " + lhs + ", count(distinct " + rhs + ") FROM joinedTable GROUP BY " +
                                lhs + " HAVING COUNT(DISTINCT " + rhs + ") > 1")
                count = cursor.fetchone()
                # if dependency is found, it is printed and also stored in a list of all dependencies
                if count == None:
                    print("Dependency found: " + lhs + " --> " + rhs)
                    dependencies.append(lhs + " --> " + rhs)
                dependenciesChecked += 1
                if dependenciesChecked % 10 == 0:
                    print("\n=====> Checked " + str(dependenciesChecked) + " dependencies. <=====")
            if dependenciesChecked >= 100:
                break

        print("\nProgram stopped after checking ~100 dependencies.")
        print("END TIME: " + str(datetime.now())[11:])
        cursor.close()