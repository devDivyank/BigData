-- GLOBAL SCHEMA 1 -> All_Movie(id, title, year, genre), which contains each movie with its main genre

-- Using non-materialized views ====>
CREATE VIEW All_Movie AS
    (SELECT id, title, year, 'Comedy' as genre FROM comedymovie
    UNION ALL
    SELECT id, title, year, 'Non-Comedy' as genre FROM noncomedymovie);

-- Using materialized views ====>
CREATE VIEW All_Movie AS
    (SELECT id, title, year, 'Comedy' as genre FROM comedymoviematerialized
    UNION ALL
    SELECT id, title, year, 'Non-Comedy' as genre FROM noncomedymoviematerialized);


-- GLOBAL SCHEMA 2 -> All_Actor(id, name, birthYear, deathYear), which contains each actor

-- Using non-materialized views ====>
CREATE VIEW All_Actor AS
    (SELECT * FROM comedyactor
    UNION ALL
    SELECT * FROM noncomedyactor);

-- Using materialized views ====>
CREATE VIEW All_Actor AS
    (SELECT * FROM comedyactormaterialized
    UNION ALL
    SELECT * FROM noncomedyactormaterialized);


-- GLOBAL SCHEMA 3 -> All_Movie_Actor(actor, movie), which stores actors participating in movies

-- Using non-materialized views ====>

CREATE VIEW All_Movie_Actor AS
(SELECT * FROM actedin);

-- Using materialized views ====>

CREATE VIEW All_Movie_Actor AS
(SELECT * FROM actedinmaterialized);
