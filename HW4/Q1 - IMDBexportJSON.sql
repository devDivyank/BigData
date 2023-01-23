-- creating a copy of member table
CREATE TABLE memberMongo AS (SELECT * FROM member);
-- replacing " with ' in the table to prevent errors while importing into mongodb
UPDATE memberMongo SET name = replace(name, '"', '''');
-- exporting as a json file
COPY (SELECT json_strip_nulls(row_to_json(t))
        FROM (SELECT id as "_id", name as "name", birthyear as "birthYear", deathyear as "deathYear"
        FROM memberMongo) t)
        TO '/Users/divyankkulshrestha/Downloads/members.json';
DROP TABLE memberMongo;

-- run the below cammand outside the mongo shell with appropriate db name, collection, filepath etc.

------>>> mongoimport --db IMDBMongoDB --collection members --file /Users/divyankkulshrestha/Downloads/members.json --drop

-- creating a copy of character, genre and titles tables
-- replacing " with ' in the tables to prevent errors while importing into mongodb
CREATE TABLE characterMongo AS (SELECT * FROM character);
UPDATE characterMongo SET character = replace(character, '"', '''');
CREATE TABLE genreMongo AS (SELECT * FROM genre);
UPDATE genreMongo SET genre = replace(genre, '"', '''');
CREATE TABLE titleMongo AS SELECT * FROM title;
UPDATE titleMongo set title = replace(title, '"', ''''), originaltitle = replace(originaltitle, '"', ''''), type = replace(type, '"', '''');
-- querying the required data and then exporting as a json file

COPY (SELECT json_strip_nulls(row_to_json(t)) FROM
(SELECT id as _id, type, title, originaltitle, startYear, endYear, runtime,
       avgrating, numvotes, genres, actors, directorid as directors, producerid as producers, writerid as writers FROM TitleMongo output
    LEFT JOIN (SELECT title as titleid, array_agg(g.genre) as genres
            FROM title_genre JOIN GenreMongo g
    ON title_genre.genre = g.id
            GROUP BY titleid ORDER BY titleid) as foo1 ON output.id = foo1.titleid
    LEFT JOIN (SELECT title as titleid, array_agg(director) as directorid
            FROM title_director GROUP BY titleid) as foo2 ON output.id = foo2.titleid
    LEFT JOIN (SELECT title as titleid, array_agg(producer) as producerid
            FROM title_producer GROUP BY titleid) as foo3 ON output.id = foo3.titleid
    LEFT JOIN (SELECT title as titleid, array_agg(writer) as writerid
            FROM title_writer GROUP BY titleid) as foo4 ON output.id = foo4.titleid
    LEFT JOIN (SELECT titleid, json_agg(actors) as actors FROM
            (SELECT foo5.titleid, json_build_object('actor', foo5.actorid, 'roles', foo5.roles) as actors FROM
            (SELECT atc.title as titleid, atc.actor as actorid, array_agg(charactermongo.character) as roles
            FROM charactermongo JOIN actor_title_character atc
    ON charactermongo.id = atc.character
            GROUP BY (atc.title, atc.actor) ORDER BY (atc.title, atc.actor)) as foo5) AS doo
            GROUP BY titleid) as foo6 ON foo6.titleid = output.id) t) to
    '/Users/divyankkulshrestha/Downloads/movies.json';

DROP TABLE characterMongo;
DROP TABLE genreMongo;
DROP TABLE titleMongo;

-- run the below cammand outside the mongo shell with appropriate db name, collection, filepath etc.

------>>> mongoimport --db IMDBMongoDB --collection movies --file /Users/divyankkulshrestha/Downloads/movies.json --drop
