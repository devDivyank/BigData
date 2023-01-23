-- USING NON-MATERIALIZED VIEWS ====>

-- QUERY 1 OPTIMIZED -> Alive actors who have participated in more than 10 movies between 2000 and 2005
-- TIME TAKEN: ~29 SECONDS
SELECT id, name, deathyear, moviecount
FROM (SELECT id, name, deathyear
      FROM comedyactor
      UNION ALL
      SELECT id, name, deathyear
      FROM noncomedyactor WHERE deathyear IS NULL) AS allactor
         JOIN
     (SELECT actor, count(actor) as moviecount
      FROM (SELECT id, allmovie.title as title, year, actor
            FROM (SELECT id, title, year
                  FROM comedymovie
                  UNION ALL
                  SELECT id, title, year
                  FROM noncomedymovie  WHERE year >= 2000
              AND year <= 2005) AS allmovie
                     JOIN actedin as allmovieactor
                     ON allmovie.id = allmovieactor.title
           ) as foo1
               JOIN (SELECT *
                     FROM comedyactor
                     UNION ALL
                     SELECT *
                     FROM noncomedyactor) AS allactor ON foo1.actor = allactor.id

      GROUP BY actor) as foo2
     ON actor = allactor.id
WHERE moviecount > 10;

-- QUERY 2 OPTIMIZED -> Actors whose name starts with “Ja” and who have never participated in any comedy movie
-- TIME TAKEN: ~26 SECONDS
SELECT *
FROM (SELECT id, name
      FROM comedyactor
      UNION ALL
      SELECT id, name
      FROM noncomedyactor) AS allactor
WHERE NOT EXISTS (SELECT *
                 FROM (SELECT actor, name
                       FROM (SELECT id, name
                             FROM comedyactor
                             UNION ALL
                             SELECT id, name
                             FROM noncomedyactor) AS allactor
                                JOIN (SELECT * FROM actedin) AS allmovieactor
                                    ON allactor.id = allmovieactor.actor
                                JOIN (SELECT id, title, year
                                      FROM comedymovie)
                           AS allmovie ON allmovieactor.title = allmovie.id) as foo1
                 WHERE foo1.actor = allactor.id)
  AND name LIKE 'Ja%';


-- USING MATERIALIZED VIEWS ====>

-- QUERY 1 OPTIMIZED -> Alive actors who have participated in more than 10 movies between 2000 and 2005
-- TIME TAKEN: 356 MS
SELECT id, name, deathyear, moviecount
FROM (SELECT id, name, deathyear
      FROM comedyactormaterialized
      UNION ALL
      SELECT id, name, deathyear
      FROM noncomedyactormaterialized) AS allactor
         JOIN
     (SELECT actor, count(actor) AS moviecount
      FROM (SELECT id, allmovie.title AS title, year, actor
            FROM (SELECT id, title, year
                  FROM comedymoviematerialized
                  UNION ALL
                  SELECT id, title, year
                  FROM noncomedymoviematerialized) AS allmovie
                     JOIN (SELECT * FROM actedinmaterialized) AS allmovieactor
                     ON allmovie.id = allmovieactor.title
            WHERE allmovie.year >= 2000
              AND allmovie.year <= 2005) AS foo1
               JOIN (SELECT *
                     FROM comedyactormaterialized
                     UNION ALL
                     SELECT *
                     FROM noncomedyactormaterialized) AS allactor ON foo1.actor = allactor.id
      WHERE allactor.deathyear IS NULL
      GROUP BY actor) AS foo2
     ON actor = allactor.id
WHERE moviecount > 10;

-- QUERY 2 OPTIMIZED -> Actors whose name starts with “Ja” and who have never participated in any comedy movie
-- TIME TAKEN: 345 MS
SELECT *
FROM (SELECT id, name
      FROM comedyactormaterialized
      UNION ALL
      SELECT id, name
      FROM noncomedyactormaterialized) AS allactor
WHERE NOT EXISTS (SELECT *
                 FROM (SELECT actor, name
                       FROM (SELECT id, name
                             FROM comedyactormaterialized
                             UNION ALL
                             SELECT id, name
                             FROM noncomedyactormaterialized) AS allactor
                                JOIN (SELECT * FROM actedinmaterialized) AS allmovieactor
                                    ON allactor.id = allmovieactor.actor
                                JOIN (SELECT id, title, year
                                      FROM comedymoviematerialized)
                           AS allmovie ON allmovieactor.title = allmovie.id) as foo1
                 WHERE foo1.actor = allactor.id)
  AND name LIKE 'Ja%';