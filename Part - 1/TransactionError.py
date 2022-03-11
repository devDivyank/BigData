"""
file: TransactionError.py
description: CSCI-620.03 - HMWK1 Q5: Failing transaction
language: python3, SQL
author: Divyank Kulshrestha, dk9924
"""

import psycopg2

host = "localhost"
dbname = "divyankkulshrestha"
user = "divyankkulshrestha"
password = None
port = 5432

if __name__ == '__main__':
    print("STARTING TRANSACTION...")

    with psycopg2.connect(host=host, dbname=dbname, user=user, password = password, port=port) as connectiontoDB:
        cursor = connectiontoDB.cursor()

        # three entries that we will try to add
        person1 = (1000000001, "Elon Dusk")
        person2 = (1000000002, "Cristiano Sonaldo")
        person3 = (1000000003, "Divyank Kulshrestha")

        # intentionally causing an error on 3rd INSERT query
        try:
            cursor.execute("INSERT INTO Person(PersonID, PrimaryName) VALUES (%s, %s)", person1)
            cursor.execute("INSERT INTO Person(PersonID, PrimaryName) VALUE (%s)", person1)    # <-- ERROR IN STATEMENT
            cursor.execute("INSERT INTO Person(PersonID, PrimaryName) VALUES (%s, %s)", person1)
            connectiontoDB.commit()
        except Exception as error:
            # exception is printed
            print("\nERROR IN ONE OR MORE QUERIES: " + str(error))
            connectiontoDB.rollback()

        # we check if any of the three entries were added in database
        cursor.execute("SELECT * FROM Person WHERE PersonID='1000000001'")
        entryOne = cursor.fetchone()
        cursor.execute("SELECT * FROM Person WHERE PersonID='1000000002'")
        entryTwo = cursor.fetchone()
        cursor.execute("SELECT * FROM Person WHERE PersonID='1000000003'")
        entryThree = cursor.fetchone()

        # if none of the three entries exist in database, no commits were made and the transaction failed
        if entryOne == None and entryTwo == None and entryThree == None:
            print("NO DATA WAS ENTERED! TRANSACTION FAILED!")
