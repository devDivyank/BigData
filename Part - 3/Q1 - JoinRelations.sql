/*
EXPLANATION ->
First off, we pick all the actor, title combinations from actor_title_character that have only one character,
which is basically all the movies where all actors only play a single character. Then we join the tables:
                        -> actor_title_character ON actor and title
                        -> member ON actor
                        -> character ON character
                        -> title_genre ON title
                        -> genre ON genreid
                        -> title ON title
We then select the required columns for records that have runtime > 90 mins.
*/

CREATE TABLE joinedtable AS
(
SELECT title.id as movieid, title.type, title.startyear, title.runtime,
        title.avgrating, t6.id as genreid, genre, memberID, birthyear, character FROM title
        JOIN (SELECT * FROM genre
                JOIN (SELECT genre as genreID, t4.title, character, memberID, birthyear FROM title_genre
                        JOIN (SELECT title, character.character, t3.id as memberID, birthyear FROM character
                               JOIN (SELECT member.id, birthyear, character, title FROM member
                                      JOIN (SELECT t1.actor, t1.title, character FROM actor_title_character
                                            JOIN (SELECT actor, title
                                                FROM actor_title_character GROUP BY (actor, title)
                                                HAVING count(*) = 1) as t1
                                            ON t1.actor = actor_title_character.actor AND
                                               t1.title = actor_title_character.title) as t2
                                      ON member.id = t2.actor) as t3
                               ON character.id = t3.character) as t4
                        ON title_genre.title = t4.title) as t5
                ON genre.id = t5.genreid) as t6
        ON title.id = t6.title
WHERE runtime >= 90
);

