CREATE TABLE memberMongo AS (SELECT * FROM member);
UPDATE memberMongo SET name = replace(name, '"', '''');
COPY (SELECT json_strip_nulls(row_to_json(t))
        FROM (SELECT id as "_id", name as "name", birthyear as "birthYear", deathyear as "deathYear"
        FROM memberMongo) t)
        TO '/Users/divyankkulshrestha/Downloads/members.json';
DROP TABLE memberMongo;
-- mongoimport --db IMDBMongoDB --collection members --file /Users/divyankkulshrestha/Downloads/members.json --drop

CREATE TABLE characterMongo AS (SELECT * FROM character);
UPDATE characterMongo SET character = replace(character, '"', '''');
CREATE TABLE genreMongo AS (SELECT * FROM genre);
UPDATE genreMongo SET genre = replace(genre, '"', '''');
CREATE TABLE titleMongo AS SELECT * FROM title;
UPDATE titleMongo set title = replace(title, '"', ''''), originaltitle = replace(originaltitle, '"', ''''), type = replace(type, '"', '''');

COPY (SELECT json_strip_nulls(row_to_json(t))
        FROM (SELECT count(*), id AS _id, type, title, originaltitle, startYear, endYear, runtime,
           avgrating, numvotes, genres, actors, directorid AS directors, producerid AS producers, writerid AS writers FROM TitleMongo T
        LEFT JOIN (SELECT title AS titleid, array_agg(g.genre) AS genres
                FROM title_genre JOIN GenreMongo g
        ON title_genre.genre = g.id
                GROUP BY titleid ORDER BY titleid) AS foo1 ON T.id = foo1.titleid
        LEFT JOIN (SELECT title as titleid, array_agg(director) as directorid
                FROM title_director GROUP BY titleid) AS foo2 ON T.id = foo2.titleid
        LEFT JOIN (SELECT title as titleid, array_agg(producer) AS producerid
                FROM title_producer GROUP BY titleid) AS foo3 ON T.id = foo3.titleid
        LEFT JOIN (SELECT title as titleid, array_agg(writer) as writerid
                FROM title_writer GROUP BY titleid) AS foo4 ON T.id = foo4.titleid
        LEFT JOIN (SELECT titleid, json_agg(actors) AS actors FROM
                (SELECT foo5.titleid, json_build_object('actor', foo5.actorid, 'roles', foo5.roles) AS actors FROM
                (SELECT atc.title as titleid, atc.actor as actorid, array_agg(charactermongo.character) AS roles
                FROM charactermongo JOIN actor_title_character atc
        ON charactermongo.id = atc.character
                GROUP BY (atc.title, atc.actor) ORDER BY (atc.title, atc.actor)) AS foo5) AS doo
                GROUP BY titleid) AS foo6 ON foo6.titleid = T.id) t)
        TO '/Users/divyankkulshrestha/Downloads/movies.json';
DROP TABLE characterMongo;
DROP TABLE genreMongo;
DROP TABLE titleMongo;
-- mongoimport --db IMDBMongoDB --collection movies --file /Users/divyankkulshrestha/Downloads/movies.json --drop

SELECT count(*) FROM title