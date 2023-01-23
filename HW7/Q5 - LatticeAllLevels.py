"""
file: LatticeAllLevels.py
description: CSCI-620.03 - HMWK7 Q5: generating all levels of the lattice (itemset mining)
language: python3
author: Divyank Kulshrestha, dk9924
"""

import psycopg2
from itertools import combinations

def actorColumnsForLevel(currentLevel):
    """
        returns the actor columns in a level as string

        :param currentLevel: level of the lattice
    """
    columns = ""
    for i in range(1, currentLevel + 1):
        columns += ', actor' + str(i)
    return columns[2:]

def selectStatement(currentLevel):
    """
        returns the SELECT part of the query for the level

        :param currentLevel: level of the lattice
    """
    statement = "SELECT"
    for i in range(1, currentLevel + 1):
        statement += " actor" + str(i) + ","
    statement += " count(movie) as count"
    return statement

def existsStatement(currentLevel):
    """
        returns the EXISTS part of the query for the level

        :param currentLevel: level of the lattice
    """
    lastLevelTable = "l" + str(currentLevel-1)
    levelView = "v" + str(currentLevel)
    fullStatement = ""
    eachExistsStatement = []
    for combination in actorAllCombinations(currentLevel):
        temp = ""
        i = 0
        for actor in combination.split(", "):
            line = lastLevelTable + "." + actorColumnsForLevel(currentLevel-1).split(', ')[i] + " = " \
                                                            + levelView + "." + actor + " AND "
            temp += line
            i += 1
        eachExistsStatement.append(temp[:-5])
    for statement in eachExistsStatement:
        fullStatement += "EXISTS (SELECT * FROM " + lastLevelTable + " WHERE " + statement + ") AND "
    return fullStatement[:-6]

def actorAllCombinations(currentLevel):
    """
        returns the combinations of actor columns for WHERE part
        of the EXISTS clauses

        :param currentLevel: level of the lattice
    """
    allActorColumns = actorColumnsForLevel(currentLevel).split(', ')
    allCombs = []
    for i in range(0, currentLevel + 1):
            for element in combinations(allActorColumns, i):
                if len(element) == currentLevel - 1:
                    allCombs.append(', '.join(element))
    return allCombs

def emptyQuery(currentLevel):
    """
        returns the query to check if a level is empty

        :param currentLevel: level of the lattice
    """
    query = "SELECT * FROM l" + str(currentLevel)
    return query

def levelTable(currentLevel):
    """
        returns the query to create the level table by combining
        different parts of the query into one query

        :param currentLevel: level of the lattice
    """
    query = "CREATE TABLE l" + str(currentLevel) + " AS SELECT * FROM (" + selectStatement(currentLevel) + " FROM v" \
             + str(currentLevel) +" WHERE " \
             + existsStatement(currentLevel) + ") GROUP BY " \
             + actorColumnsForLevel(currentLevel) + ") AS foo WHERE count >= 5;"
    return query

def joinStatementAsView(currentLevel):
    """
        returns a query to create a view of all the JOINS required to create the level table

        :param currentLevel: level of the lattice
    """
    if currentLevel == 2:
        query = '''CREATE VIEW v2 AS (SELECT p1.movie as movie, p1.actor as actor1, p2.actor AS actor2
                   FROM popular_movie_actors p1
                   JOIN popular_movie_actors p2 ON p1.movie = p2.movie WHERE p1.actor > p2.actor);'''
        return query
    else:
        query = "CREATE VIEW v" + str(currentLevel) + " AS (SELECT v" + str(currentLevel-1) + ".movie, " + actorColumnsForLevel(currentLevel)[:-6] + \
                                "p" + str(currentLevel) + ".actor AS actor" + str(currentLevel) + " FROM v" + str(currentLevel-1) + " JOIN popular_movie_actors p" \
                                + str(currentLevel) + " ON p" + str(currentLevel) + ".movie = v" + str(currentLevel-1) + ".movie AND actor" \
                                + str(currentLevel-1) + " > p" + str(currentLevel) + ".actor);"
        return query

# VARIABLES TO SET FOR CONNECTING TO DATABASE
host = "localhost"
dbname = "divyankkulshrestha"
user = "divyankkulshrestha"
password = None
port = 5432

if __name__ == '__main__':
    # connection to database
    with psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port) as connectionToDB:
        cursor = connectionToDB.cursor()

        currentLevel = 1
        emptyLevel = False
        while not emptyLevel:
            if currentLevel == 1:
                query = '''CREATE TABLE L1 AS
                                (SELECT * FROM
                                    (SELECT actor AS actor1, count(movie) AS MovieCount
                                FROM popular_movie_actors GROUP BY actor1) as foo1
                            WHERE foo1.MovieCount >= 5);'''
                cursor.execute(query)
                # to check if level is empty
                emptinessQuery = emptyQuery(1)
                cursor.execute(emptinessQuery)
                output = cursor.fetchone()
                # if level is empty
                if output is None:
                    print("l" + str(currentLevel) + " is the empty level.")
                    print("l" + str(currentLevel - 1) + " is the last non-empty level.")
                    countQuery = "SELECT COUNT(*) FROM l" + str(currentLevel - 1) + ";"
                    cursor.execute(countQuery)
                    output = cursor.fetchone()
                    print("Items in last non-empty level : " + str(output[0]))
                    emptyLevel = True
                # if level is not empty
                else:
                    print("l" + str(currentLevel) + " is not empty.")
                    countQuery = "SELECT COUNT(*) FROM l" + str(currentLevel) + ";"
                    cursor.execute(countQuery)
                    output = cursor.fetchone()
                    print("Items in this level : " + str(output[0]))
                currentLevel += 1
            else:
                # creating a view of JOINS required
                query = joinStatementAsView(currentLevel)
                cursor.execute(query)
                # creating the level table
                levelTableQuery = levelTable(currentLevel)
                cursor.execute(levelTableQuery)
                # to check if level is empty
                emptinessQuery = emptyQuery(currentLevel)
                cursor.execute(emptinessQuery)
                output = cursor.fetchone()
                # if level is empty
                if output is None:
                    print("l" + str(currentLevel) + " is the empty level.")
                    print("l" + str(currentLevel-1) + " is the last non-empty level.")
                    countQuery = "SELECT COUNT(*) FROM l" + str(currentLevel-1) + ";"
                    cursor.execute(countQuery)
                    output = cursor.fetchone()
                    print("Items in last non-empty level : " + str(output[0]))
                    emptyLevel = True
                # if level is not empty
                else:
                    print("l" + str(currentLevel) + " is not empty.")
                    countQuery = "SELECT COUNT(*) FROM l" + str(currentLevel) + ";"
                    cursor.execute(countQuery)
                    output = cursor.fetchone()
                    print("Items in this level : " + str(output[0]))
                currentLevel += 1

        cursor.close()
