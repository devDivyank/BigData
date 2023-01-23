"""
file: ModelIMDB.py
description: CSCI-620.03 - HMWK1 Q4: Modelling IMDB database
language: python3, SQL
author: Divyank Kulshrestha, dk9924
"""

import psycopg2
import gzip
from datetime import datetime

# VARIABLES TO SET FOR CONNECTING TO DATABASE AND PROVIDING DATA FILES BEFORE RUNNING
host = "localhost"
dbname = "divyankkulshrestha"
user = "divyankkulshrestha"
password = None
port = 5432
fileTitleBasics = "/Users/divyankkulshrestha/Downloads/BigData HW1/title.basics.tsv.gz"
fileTitleRatings = "/Users/divyankkulshrestha/Downloads/BigData HW1/title.ratings.tsv.gz"
fileNameBasicsTSV = "/Users/divyankkulshrestha/Downloads/BigData HW1/name.basics.tsv"
fileTitlePrincipalsTSV = '/Users/divyankkulshrestha/Downloads/BigData HW1/title.principals.tsv'

if __name__ == '__main__':

    print("START TIME: " + str(datetime.now())[11:])

    #connection to database
    with psycopg2.connect(host = host, dbname = dbname, user = user, password = password, port = port) as connectiontoDB:
        cursor = connectiontoDB.cursor()

        # creating Movie table
        print("\nCREATING 'MOVIE' TABLE...")
        cursor.execute("DROP TABLE IF EXISTS Movie")
        cursor.execute('''CREATE TABLE Movie (
                                        TitleID INT PRIMARY KEY,
                                        PrimaryTitle VARCHAR(242) NOT NULL,
                                        Genres VARCHAR(80));
                                        ''')
        # reading data from TitleBasics file and adding titleid, primarytitle and genre if non-adult movie is found
        with gzip.open(fileTitleBasics, "rt") as titleBasics:
            header = True
            for line in titleBasics:
                if header:
                    header = False
                    continue
                rowList = line.split('\t')
                # slicing and casting the titleID into an integer
                titleID, titleType, primaryTitle, isAdult, genres = \
                int(rowList[0][2:]), rowList[1], rowList[2], int(rowList[4]), rowList[8][:-1].split(',')
                if titleType == "movie" and isAdult == 0:
                    insertMovie = 'INSERT INTO Movie (TitleID, PrimaryTitle, Genres) VALUES (%s, %s, %s)'
                    rowValues = (titleID, primaryTitle, genres)
                    cursor.execute(insertMovie, rowValues)

        connectiontoDB.commit()
        print("MOVIE TABLE CREATED: " + str(datetime.now())[11:])

        # creating Rating table
        print("\nCREATING 'RATING' TABLE...")
        cursor.execute("DROP TABLE IF EXISTS Rating")
        cursor.execute('''CREATE TABLE Rating (
                                        TitleID INT PRIMARY KEY,
                                        Rating FLOAT,
                                        Votes INT);''')

        # reading data from TitleRatings file and adding titleid, rating and votes
        with gzip.open(fileTitleRatings, "rt") as titleRatings:
            header = True
            for line in titleRatings:
                if header:
                    header = False
                    continue
                rowList = line.split('\t')

                # slicing and casting the titleID into an integer
                titleID, rating, votes = int(rowList[0][2:]), float(rowList[1]), int(rowList[2])
                insertRating = 'INSERT INTO Rating (TitleID, Rating, Votes) VALUES (%s, %s, %s)'
                rowValues = (titleID, rating, votes)
                cursor.execute(insertRating, rowValues)

        # adding a foreign key to Movie table named 'Rating' which points to TitleId in the Rating table
        cursor.execute("ALTER TABLE Movie ADD COLUMN Rating INT;")
        cursor.execute("ALTER TABLE Movie ADD FOREIGN KEY (Rating) REFERENCES Rating(TitleID);")

        # checking if rating for a TitleId actually exists in Rating table using 'EXISTS' clause
        cursor.execute("UPDATE Movie SET Rating = TitleID WHERE EXISTS (Select * FROM Rating WHERE Movie.titleID = Rating.TitleID);")

        connectiontoDB.commit()
        print("RATING TABLE CREATED: " + str(datetime.now())[11:])

        # creating Person table
        print("\nCREATING 'PERSON' TABLE...")
        cursor.execute("DROP TABLE IF EXISTS Person")
        cursor.execute('''CREATE TABLE Person (
                                        PersonID VARCHAR PRIMARY KEY,
                                        PrimaryName VARCHAR NOT NULL,
                                        birth VARCHAR,
                                        death VARCHAR,
                                        Profession VARCHAR,
                                        KnownFor VARCHAR);''')

        # using copy command to move data from Name.Basics.tsv into the Person table
        cursor.execute("COPY Person FROM '" + fileNameBasicsTSV + "' with (format csv, delimiter E'\t', header True, QUOTE E'\b', NULL '\\N');")
        # dropping the columns that are not needed (birth, death etc.)
        cursor.execute("ALTER TABLE Person DROP COLUMN birth, DROP COLUMN death, DROP COLUMN Profession, DROP COLUMN KnownFor;")

        # slicing and casting the PersonID into an integer
        cursor.execute("UPDATE Person SET PersonID=replace(PersonID, 'nm', '')")
        cursor.execute("ALTER TABLE Person ALTER COLUMN PersonID TYPE INT USING PersonID::Integer")

        connectiontoDB.commit()
        print("PERSON TABLE CREATED: " + str(datetime.now())[11:])

        # creating a dummy table to store all the crew info
        print("\nCREATING A DUMMY CREW TABLE...")
        cursor.execute("DROP TABLE IF EXISTS TitlePrincipals;")
        cursor.execute('''CREATE TABLE TitlePrincipals (TitleID VARCHAR,
                                                        ordering INT,
                                                        PersonID VARCHAR,
                                                        category VARCHAR,
                                                        job VARCHAR,
                                                        characters VARCHAR);
                                                        ''')

        # using copy command to move data from Title.Principals.tsv into the dummy table
        cursor.execute("COPY TitlePrincipals FROM '" + fileTitlePrincipalsTSV + "' with (format csv, delimiter E'\t', header True, QUOTE E'\b', NULL '\\N');")
        # dropping the columns that are not needed (order, character etc.)
        cursor.execute("ALTER TABLE TitlePrincipals DROP COLUMN ordering, DROP COLUMN characters, DROP COLUMN job;")

        # slicing and casting the TitleId and PersonID into an integer
        cursor.execute("UPDATE TitlePrincipals SET TitleID=replace(TitleID, 'tt', '')")
        cursor.execute("UPDATE TitlePrincipals SET PersonID=replace(PersonID, 'nm', '')")
        cursor.execute("ALTER TABLE TitlePrincipals ALTER COLUMN TitleID TYPE INT USING TitleID::Integer")
        cursor.execute("ALTER TABLE TitlePrincipals ALTER COLUMN PersonID TYPE INT USING PersonID::Integer")

        connectiontoDB.commit()
        print("CREW TABLE CREATED: " + str(datetime.now())[11:])

        # splitting data in dummy table into actors, writers, directors, producers
        print("\nSPLITTING DATA FROM CREW INTO ACTOR, WRITER, DIRECTOR & PRODUCER TABLES...")
        # creating Actor table
        cursor.execute('''CREATE TABLE Actor (
                                            TitleID INT,
                                            PersonID INT,
                                            PRIMARY KEY (TitleID, PersonID),
                                            FOREIGN KEY (TitleID) REFERENCES Movie(TitleID),
                                            FOREIGN KEY (PersonID) REFERENCES Person(PersonID)
                                            );''')
        # creating Writer table
        cursor.execute('''CREATE TABLE Writer (
                                            TitleID INT,
                                            PersonID INT,
                                            PRIMARY KEY (TitleID, PersonID),
                                            FOREIGN KEY (TitleID) REFERENCES Movie(TitleID),
                                            FOREIGN KEY (PersonID) REFERENCES Person(PersonID)
                                            );''')
        # creating Director table
        cursor.execute('''CREATE TABLE Director (
                                            TitleID INT,
                                            PersonID INT,
                                            PRIMARY KEY (TitleID, PersonID),
                                            FOREIGN KEY (TitleID) REFERENCES Movie(TitleID),
                                            FOREIGN KEY (PersonID) REFERENCES Person(PersonID)
                                            );''')
        # creating Producer table
        cursor.execute('''CREATE TABLE Producer (
                                            TitleID INT,
                                            PersonID INT,
                                            PRIMARY KEY (TitleID, PersonID),
                                            FOREIGN KEY (TitleID) REFERENCES Movie(TitleID),
                                            FOREIGN KEY (PersonID) REFERENCES Person(PersonID)
                                            );''')

        # using EXISTS to check for invalid keys and DISTINCT to only add distinct (TitleID, PersonID) pairs from dummy table
        cursor.execute("INSERT INTO Actor (SELECT DISTINCT TitleID, PersonID FROM TitlePrincipals WHERE category = 'actor'"
                       " AND EXISTS (SELECT * FROM Movie WHERE Movie.TitleID = TitlePrincipals.TitleID)"
                       " AND EXISTS (SELECT * FROM Person WHERE Person.PersonID = TitlePrincipals.PersonID));")

        cursor.execute("INSERT INTO Writer (SELECT DISTINCT TitleID, PersonID FROM TitlePrincipals WHERE category = 'writer'"
                       " AND EXISTS (SELECT * FROM Movie WHERE Movie.TitleID = TitlePrincipals.TitleID)"
                       " AND EXISTS (SELECT * FROM Person WHERE Person.PersonID = TitlePrincipals.PersonID));")

        cursor.execute("INSERT INTO Director (SELECT DISTINCT TitleID, PersonID FROM TitlePrincipals WHERE category = 'director'"
                       " AND EXISTS (SELECT * FROM Movie WHERE Movie.TitleID = TitlePrincipals.TitleID)"
                       " AND EXISTS (SELECT * FROM Person WHERE Person.PersonID = TitlePrincipals.PersonID));")

        cursor.execute("INSERT INTO Producer (SELECT DISTINCT TitleID, PersonID FROM TitlePrincipals WHERE category = 'producer'"
                       " AND EXISTS (SELECT * FROM Movie WHERE Movie.TitleID = TitlePrincipals.TitleID)"
                       " AND EXISTS (SELECT * FROM Person WHERE Person.PersonID = TitlePrincipals.PersonID));")

        # dropping the dummy table at the end
        cursor.execute("DROP TABLE TitlePrincipals")
        connectiontoDB.commit()
        print("DATA SPLIT SUCCESSFULLY!: " + str(datetime.now())[11:])

        cursor.close()

    print("\nDONE: " + str(datetime.now())[11:])
