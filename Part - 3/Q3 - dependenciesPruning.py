"""
file: dependenciesPruning.py
description: CSCI-620.03 - HMWK3 Q3: finding dependencies using pruning
language: python3, SQL
author: Divyank Kulshrestha, dk9924
"""
from itertools import combinations
import psycopg2
import pandas.io.sql as pandasql
from datetime import datetime

# VARIABLES TO SET FOR CONNECTING TO DATABASE
host = "localhost"
dbname = "divyankkulshrestha"
user = "divyankkulshrestha"
password = None
port = 5432

def prunedPowerSet(columnList):
    """
        function that returns all combinations of columns of size 2 or less

        :param columnList: a list of all columns
    """
    combs = []
    n = len(columnList)
    for i in range(0, n + 1):
        for element in combinations(columnList, i):
            combs.append(', '.join(element))
    prunedSet = []
    for s in combs[1:]:
        if len(s.split(", ")) <= 2:
            prunedSet.append(s)
    return prunedSet


def infer(depToCheck):
    """
        function to check if a dependency can be inferred through pruning

        :param depToCheck: left and right side column(s) of the dependency to be checked
    """
    for dependency in allDependencies:
        # if rhs columns are same and lhs columns of original dependency are a subset of depToCheck
        if dependency[1] == depToCheck[1] and set(dependency[0]).issubset(set(depToCheck[0])):
            originalDependency = str(dependency[0]) + " --> " + str(dependency[1])
            return [True, originalDependency]
    return [False]


def checkDependency(depToCheck):
    """
        function to check if the dependency 'lhs --> rhs' exists

        :param depToCheck: left and right side column(s) of the dependency to be checked
    """
    lhs = depToCheck[0]
    rhs = depToCheck[1]
    combined = lhs.copy()
    combined.append(rhs)

    lhsPartitions = data.groupby(lhs, dropna=False)
    combinedPartitons = data.groupby(combined, dropna=False)
    lhsPartitionsLength = len(lhsPartitions.indices.keys())
    combinedPartitionsLength = len(combinedPartitons.indices.keys())

    return lhsPartitionsLength == combinedPartitionsLength

if __name__ == '__main__':
    print("START TIME: " + str(datetime.now())[11:])
    columns = ["movieid", "type", "startyear", "runtime", "avgrating", "genreid", "genre",
               "memberid", "birthyear", "character"]

    # connection to database
    with psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port) as connectionToDB:
        allDependencies = []
        allCombinations = prunedPowerSet(columns)
        # moving data from joinedTable into a pandas dataframe
        data = pandasql.read_sql('''SELECT * FROM joinedTable''', connectionToDB)
        allPartitions = {}

        # choosing the lhs of dependency to be checked (from the pruned power set)
        for lhs in allCombinations:
            # choosing the rhs of dependency to be checked (only one column on rhs)
            for rhs in columns:
                depToCheck = (lhs.split(", "), rhs)
                # trivial dependency skipped
                if lhs == rhs or (len(lhs.split(',')) > 1 and rhs in lhs):
                    continue
                # if current dependency can be inferred from an earlier dependency, we can 'prune' the check for it
                if infer(depToCheck)[0]:
                    # the dependency used for making the inference
                    originalDep = infer(depToCheck)[1]
                    chars = "[']"
                    # formatting, solely for printing the dependency
                    for char in chars:
                        originalDep = originalDep.replace(char, "")
                    print(lhs + " --> " + rhs + "   ======> INFERRED FROM ======>   " + str(originalDep))
                else:
                # if dependency cannot be inferred from earlier dependencies
                    exists = checkDependency(depToCheck)
                    # if dependency exists
                    if exists:
                        print(lhs + " --> " + rhs)
                        allDependencies.append(depToCheck)

    print("\nEND TIME: " + str(datetime.now())[11:])
