CREATE TABLE moviesover75 AS (
    SELECT * FROM title WHERE runtime > 75 AND type='movie'
);
ALTER TABLE moviesover75 ADD PRIMARY KEY (id);
SELECT count(*) FROM actedin;

-- SOURCE 1 -> ComedyMovie(id, title, year), which stores comedy movies

CREATE VIEW ComedyMovie AS (SELECT moviesover75.id as id, moviesover75.title as title, moviesover75.startyear as year
    FROM moviesover75 JOIN title_genre ON moviesover75.id = title_genre.title
                      JOIN genre ON title_genre.genre = genre.id
    WHERE genre.genre = 'Comedy');

CREATE MATERIALIZED VIEW ComedyMovieMaterialized AS ( SELECT moviesover75.id as id, moviesover75.title as title, moviesover75.startyear as year
    FROM moviesover75 JOIN title_genre ON moviesover75.id = title_genre.title
                      JOIN genre ON title_genre.genre = genre.id
    WHERE genre.genre = 'Comedy');

-- SOURCE 2 -> NonComedyMovie(id, title, year), which stores movies that are not
--              comedies (there is no comedy genre related to them)

CREATE VIEW NonComedyMovie AS (SELECT id, moviesover75.title, startyear as year FROM moviesover75
        LEFT JOIN (SELECT title,1 AS isComedy FROM title_genre WHERE genre=23 group by TITLE) AS foo1
        ON moviesover75.id = foo1.title WHERE (coalesce(isComedy,0)) = 0);

CREATE MATERIALIZED VIEW NonComedyMovieMaterialized AS (SELECT id, moviesover75.title, startyear as year FROM moviesover75
        LEFT JOIN (SELECT title,1 AS isComedy FROM title_genre WHERE genre=23 group by TITLE) AS foo1
        ON moviesover75.id = foo1.title WHERE (coalesce(isComedy,0))=0);

-- SOURCE 3 -> ComedyActor(id, name, birthYear, deathYear), which stores actors who
--              have participated in at least a comedy movie

CREATE VIEW ComedyActor AS (SELECT member.id, member.name, member.birthyear, member.deathyear FROM ComedyMovie
    JOIN title_actor ON title_actor.title = ComedyMovie.id
    JOIN member ON member.id = title_actor.actor
GROUP BY member.id, member.name, member.birthyear, member.deathyear);

CREATE MATERIALIZED VIEW ComedyActorMaterialized AS (SELECT member.id, member.name, member.birthyear, member.deathyear FROM ComedyMovie
    JOIN title_actor ON title_actor.title = ComedyMovie.id
    JOIN member ON member.id = title_actor.actor
GROUP BY member.id, member.name, member.birthyear, member.deathyear);

-- SOURCE 4 -> NonComedyActor(id, name, birthYear, deathYear), which stores actors who
--              have never participated in any comedy movie

CREATE VIEW NonComedyActor AS (SELECT member.id, member.name,  member.birthyear, member.deathyear FROM title_actor
        JOIN member ON title_actor.actor = member.id
        WHERE EXISTS(SELECT * FROM moviesover75 WHERE moviesover75.id = title_actor.title)
        AND NOT EXISTS(SELECT * FROM (SELECT * FROM ComedyMovie JOIN title_actor ta ON ComedyMovie.id = ta.title)
                                    as foo1 WHERE member.id = foo1.actor)
GROUP BY member.id, member.name,  member.birthyear, member.deathyear);

CREATE MATERIALIZED VIEW NonComedyActorMaterialized AS (SELECT member.id, member.name,  member.birthyear, member.deathyear FROM title_actor
        JOIN member ON title_actor.actor = member.id
        WHERE EXISTS(SELECT * FROM moviesover75 WHERE moviesover75.id = title_actor.title)
        AND NOT EXISTS(SELECT * FROM (SELECT * FROM ComedyMovie JOIN title_actor ta ON ComedyMovie.id = ta.title)
                                    as foo1 WHERE member.id = foo1.actor)
GROUP BY member.id, member.name,  member.birthyear, member.deathyear);

-- SOURCE 5 -> ActedIn(actor, movie), which stores all actors participation in movies

CREATE VIEW ActedIn AS (SELECT title_actor.actor AS actor, title_actor.title AS title FROM title_actor
        JOIN moviesover75 ON moviesover75.id = title_actor.title);

CREATE MATERIALIZED VIEW ActedInMaterialized AS (SELECT title_actor.actor AS actor, title_actor.title AS title FROM title_actor
        JOIN moviesover75 ON moviesover75.id = title_actor.title);

