"""
file: FullModelIMDB.py
description: CSCI-620.03 - HMWK2 Q1: Modelling the full IMDB database
language: python3, SQL
author: Divyank Kulshrestha, dk9924
"""

import psycopg2
from datetime import datetime

# VARIABLES TO SET FOR CONNECTING TO DATABASE AND PROVIDING DATA FILES BEFORE RUNNING
host = "localhost"
dbname = "divyankkulshrestha"
user = "divyankkulshrestha"
password = None
port = 5432
fileTitleBasics = "/Users/divyankkulshrestha/Downloads/BigData HW1/title.basics.tsv"
fileTitleRatings = "/Users/divyankkulshrestha/Downloads/BigData HW1/title.ratings.tsv"
fileNameBasics = "/Users/divyankkulshrestha/Downloads/BigData HW1/name.basics.tsv"
fileTitlePrincipals = '/Users/divyankkulshrestha/Downloads/BigData HW1/title.principals.tsv'

if __name__ == '__main__':
    print("START TIME: " + str(datetime.now())[11:])

    # connection to database
    with psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port) as connectionToDB:
        cursor = connectionToDB.cursor()

        # creating Title table
        print("\nCREATING 'Title' TABLE..." + str(datetime.now())[11:])
        cursor.execute("DROP TABLE IF EXISTS Title")
        cursor.execute('''CREATE TABLE Title (  id VARCHAR PRIMARY KEY,
                                                type VARCHAR,
                                                title VARCHAR,
                                                originalTitle VARCHAR,
                                                isAdult INT,
                                                startYear INT,
                                                endYear INT,
                                                runTime INT,
                                                Genres VARCHAR );
                                                ''')
        # using copy command to move data from Title.Basics.tsv into the Title table
        cursor.execute("COPY Title FROM '" + fileTitleBasics + "' with (format csv, delimiter E'\t', header True, QUOTE E'\b', NULL '\\N');")
        cursor.execute("DELETE FROM Title WHERE isAdult=1")
        cursor.execute("ALTER TABLE Title DROP COLUMN isAdult")
        cursor.execute("UPDATE Title SET id=replace(id, 'tt', '')")
        cursor.execute("ALTER TABLE Title ALTER COLUMN id TYPE INT USING id::Integer")

        connectionToDB.commit()

        # adding ratings to title table
        print("\nADDING RATINGS TO 'Title' TABLE..." + str(datetime.now())[11:])
        cursor.execute("DROP TABLE IF EXISTS dummyRating")
        cursor.execute('''CREATE TABLE dummyRating (
                                                TitleID VARCHAR PRIMARY KEY,
                                                avgRating FLOAT,
                                                numVotes INT);''')
        # using copy command to move data from Title.Ratings.tsv into a dummy table
        cursor.execute("COPY dummyRating FROM '" + fileTitleRatings + "' with (format csv, delimiter E'\t', header True, QUOTE E'\b', NULL '\\N');")
        # slicing and casting the TitleId into an integer
        cursor.execute("UPDATE dummyRating SET TitleID=replace(TitleID, 'tt', '')")
        cursor.execute("ALTER TABLE dummyRating ALTER COLUMN TitleID TYPE INT USING TitleID::Integer")

        # moving data from dummy table into title table
        cursor.execute("ALTER TABLE Title ADD avgRating FLOAT")
        cursor.execute("ALTER TABLE Title ADD numVotes INT")
        cursor.execute('''UPDATE Title SET avgRating=dummyRating.avgRating, numVotes=dummyRating.numVotes
                                           FROM dummyRating
                                           WHERE Title.id=dummyRating.TitleID;''')
        cursor.execute("DROP TABLE dummyRating")

        connectionToDB.commit()

        # creating genre table
        print("\nCREATING 'Genre' TABLE..." + str(datetime.now())[11:])
        cursor.execute("DROP TABLE IF EXISTS dummygenre")
        cursor.execute('''CREATE TABLE dummygenre ( genre VARCHAR );''')

        # unnesting all genre arrays and inserting DISTINCT values into Genre table
        cursor.execute("INSERT INTO dummyGenre(genre) SELECT DISTINCT (unnest(string_to_array(Genres, ','))) FROM Title")
        cursor.execute("DROP TABLE IF EXISTS Genre")
        cursor.execute('''CREATE TABLE Genre ( id SERIAL PRIMARY KEY,
                                               genre VARCHAR );
                                               ''')
        cursor.execute("INSERT INTO Genre(genre) SELECT genre FROM dummyGenre")
        cursor.execute("DROP TABLE dummyGenre")

        connectionToDB.commit()

        # creating Title_Genre table
        print("\nCREATING 'Title_Genre' TABLE..." + str(datetime.now())[11:])
        cursor.execute("DROP TABLE IF EXISTS Title_Genre")
        cursor.execute('''CREATE TABLE Title_Genre (genre INT,
                                                    title INT,
                                                    PRIMARY KEY (genre, title),
                                                    FOREIGN KEY (genre) REFERENCES Genre(id),
                                                    FOREIGN KEY (title) REFERENCES Title(id));
                                                    ''')
        cursor.execute("DROP TABLE IF EXISTS dummygenre")
        cursor.execute('''CREATE TABLE dummygenre ( titleid INT,
                                                    genre VARCHAR);
                                                    ''')
        # unnesting Genres for a title and insert title and genre into Title_Genre
        cursor.execute("INSERT INTO dummygenre(titleid, genre) SELECT id, (unnest(string_to_array(Genres, ','))) FROM Title")
        cursor.execute('''INSERT INTO Title_Genre(genre, title) SELECT Genre.id, dummygenre.titleid
                                        FROM Genre, dummygenre WHERE dummygenre.genre=Genre.genre''')
        cursor.execute("DROP TABLE dummygenre")
        cursor.execute("ALTER TABLE Title DROP COLUMN Genres")

        connectionToDB.commit()

        # creating Member table
        print("\nCREATING 'Member' TABLE..." + str(datetime.now())[11:])
        cursor.execute("DROP TABLE IF EXISTS Member")
        cursor.execute('''CREATE TABLE Member ( id VARCHAR PRIMARY KEY,
                                                name VARCHAR NOT NULL,
                                                birthyear VARCHAR,
                                                deathyear VARCHAR,
                                                Profession VARCHAR,
                                                KnownFor VARCHAR);''')
        # using copy command to move data from Name.Basics.tsv into the Member table
        cursor.execute("COPY Member FROM '" + fileNameBasics + "' with (format csv, delimiter E'\t', header True, QUOTE E'\b', NULL '\\N');")
        # dropping the columns that are not needed (profession, knownfor)
        cursor.execute("ALTER TABLE Member DROP COLUMN Profession, DROP COLUMN KnownFor;")
        cursor.execute("UPDATE Member SET id=replace(id, 'nm', '')")
        cursor.execute("ALTER TABLE Member ALTER COLUMN id TYPE INT USING id::Integer")
        connectionToDB.commit()

        # creating a dummy table to store all the crew info
        print("\nCREATING A DUMMY CREW TABLE..." + str(datetime.now())[11:])
        cursor.execute("DROP TABLE IF EXISTS TitlePrincipals;")
        cursor.execute('''CREATE TABLE TitlePrincipals (TitleID VARCHAR,
                                                        ordering INT,
                                                        PersonID VARCHAR,
                                                        category VARCHAR,
                                                        job VARCHAR,
                                                        characters VARCHAR);
                                                        ''')
        # using copy command to move data from Title.Principals.tsv into the dummy table
        cursor.execute("COPY TitlePrincipals FROM '" + fileTitlePrincipals + "' with (format csv, delimiter E'\t', header True, QUOTE E'\b', NULL '\\N');")
        # dropping the columns that are not needed (ordering, job etc.)
        cursor.execute("ALTER TABLE TitlePrincipals DROP COLUMN ordering, DROP COLUMN job;")
        # slicing and casting the TitleId and PersonID into an integer
        cursor.execute("UPDATE TitlePrincipals SET TitleID=replace(TitleID, 'tt', '')")
        cursor.execute("ALTER TABLE TitlePrincipals ALTER COLUMN TitleID TYPE INT USING TitleID::Integer")
        cursor.execute("UPDATE TitlePrincipals SET PersonID=replace(PersonID, 'nm', '')")
        cursor.execute("ALTER TABLE TitlePrincipals ALTER COLUMN PersonID TYPE INT USING PersonID::Integer")

        # trimming the brackets around the genre array
        cursor.execute("UPDATE TitlePrincipals SET characters=replace(characters, '[\"', '');")
        cursor.execute("UPDATE TitlePrincipals SET characters=replace(characters, '\"]', '');")

        connectionToDB.commit()

        # splitting data in dummy table into actors, writers, directors, producers
        print("\nSPLITTING DATA FROM CREW INTO ACTOR, WRITER, DIRECTOR & PRODUCER..." + str(datetime.now())[11:])
        # creating Title_Producer table
        print("Adding producers...")
        cursor.execute('''CREATE TABLE Title_Producer AS SELECT DISTINCT personID AS Producer, titleID AS Title
                            FROM titlePrincipals WHERE category = 'producer'
                                          AND EXISTS(SELECT * FROM Member WHERE Member.id = titlePrincipals.personID)
                                          AND EXISTS(SELECT * FROM Title WHERE Title.id = titlePrincipals.titleID);
                            ALTER TABLE Title_Producer ADD PRIMARY KEY (Producer, Title);
                            ALTER TABLE Title_Producer ADD FOREIGN KEY (Producer) REFERENCES Member (id);
                            ALTER TABLE Title_Producer ADD FOREIGN KEY (Title) REFERENCES Title (id);''')
        connectionToDB.commit()

        # creating Title_Director table
        print("Adding directors...")
        cursor.execute('''CREATE TABLE Title_Director AS SELECT DISTINCT personID AS Director, titleID AS Title
                            FROM titlePrincipals WHERE category = 'director'
                                            AND EXISTS(SELECT * FROM Member WHERE Member.id = titlePrincipals.personID)
                                            AND EXISTS(SELECT * FROM Title WHERE Title.id = titlePrincipals.titleID);
                            ALTER TABLE Title_Director ADD PRIMARY KEY (Director, Title);
                            ALTER TABLE Title_Director ADD FOREIGN KEY (Director) REFERENCES Member (id);
                            ALTER TABLE Title_Director ADD FOREIGN KEY (Title) REFERENCES Title (id);''')
        connectionToDB.commit()

        # creating Title_Writer table
        print("Adding writers...")
        cursor.execute('''CREATE TABLE Title_Writer AS SELECT DISTINCT personID AS Writer, titleID AS Title
                            FROM titlePrincipals WHERE category = 'writer'
                                            AND EXISTS(SELECT * FROM Member WHERE Member.id = titlePrincipals.personID)
                                            AND EXISTS(SELECT * FROM Title WHERE Title.id = titlePrincipals.titleID);
                            ALTER TABLE Title_Writer ADD PRIMARY KEY (Writer, Title);
                            ALTER TABLE Title_Writer ADD FOREIGN KEY (Writer) REFERENCES Member (id);
                            ALTER TABLE Title_Writer ADD FOREIGN KEY (Title) REFERENCES Title (id);''')
        connectionToDB.commit()

        # creating Title_Actor table
        print("Adding actors/actresses...")
        cursor.execute('''CREATE TABLE Title_Actor AS SELECT DISTINCT personID AS Actor, titleID AS Title
                                FROM titlePrincipals WHERE (category = 'actor' OR category = 'actress')
                                                AND EXISTS(SELECT * FROM Member WHERE Member.id = titlePrincipals.personID)
                                                AND EXISTS(SELECT * FROM Title WHERE Title.id = titlePrincipals.titleID);
                            ALTER TABLE Title_Actor ADD PRIMARY KEY (Actor, Title);
                            ALTER TABLE Title_Actor ADD FOREIGN KEY (Actor) REFERENCES Member (id);
                            ALTER TABLE Title_Actor ADD FOREIGN KEY (Title) REFERENCES Title (id);''')
        # using EXISTS to check for invalid keys and DISTINCT to only add distinct (TitleID, PersonID) pairs from dummy table

        connectionToDB.commit()

        # creating Character table
        print("\nCREATING 'Character' TABLE..." + str(datetime.now())[11:])
        cursor.execute("DROP TABLE IF EXISTS Character")
        cursor.execute("CREATE TABLE Character ( id SERIAL PRIMARY KEY, character VARCHAR );")
        cursor.execute('''INSERT INTO Character(character) (SELECT DISTINCT unnest(string_to_array(characters, '","'))
                                AS character FROM titleprincipals);''')
        connectionToDB.commit()

        # creating Actor_Title_Character table
        print("\nCREATING 'Actor_Title_Character' TABLE..." + str(datetime.now())[11:])
        cursor.execute("DROP TABLE IF EXISTS Actor_Title_Character")
        cursor.execute('''CREATE TABLE Actor_Title_Character ( actor INT,
                                       title INT,
                                       character INT,
                                       PRIMARY KEY (actor, title, character),
                                       FOREIGN KEY (actor, title) REFERENCES title_actor(actor, title),
                                       FOREIGN KEY (character) REFERENCES Character(id)
                                       );''')
        cursor.execute('''INSERT INTO Actor_Title_Character(actor, title, character)
                                SELECT DISTINCT output.personid, output.titleid, character.id
                                FROM ((SELECT titleid, personid, unnest(string_to_array(characters, '","')) as character from titleprincipals)
                                as output JOIN character on output.character = character.character)
                                JOIN title_actor ON output.titleid = title_actor.title AND output.personid = title_actor.actor;
                          ''')
        cursor.execute("DROP TABLE TitlePrincipals")
        connectionToDB.commit()

        cursor.close()

    print("\nDONE: " + str(datetime.now())[11:])
