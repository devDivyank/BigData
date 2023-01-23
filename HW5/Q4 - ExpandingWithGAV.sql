-- USING NON-MATERIALIZED VIEWS ====>

-- QUERY 1 -> Alive actors who have participated in more than 10 movies between 2000 and 2005
-- TIME TAKEN: ~32 SECONDS
SELECT id, name, deathyear, moviecount
FROM (SELECT *
      FROM comedyactor
      UNION ALL
      SELECT *
      FROM noncomedyactor) AS allactor
         JOIN
     (SELECT actor, count(actor) as moviecount
      FROM (SELECT id, allmovie.title as title, year, actor
            FROM (SELECT id, title, year, 'Comedy' as genre
                  FROM comedymovie
                  UNION ALL
                  SELECT id, title, year, 'Non-Comedy' as genre
                  FROM noncomedymovie) AS allmovie
                     JOIN (SELECT * FROM actedin) as allmovieactor
                         ON allmovie.id = allmovieactor.title
            WHERE allmovie.year >= 2000
              AND allmovie.year <= 2005) as foo1
               JOIN (SELECT *
                     FROM comedyactor
                     UNION ALL
                     SELECT *
                     FROM noncomedyactor) AS allactor ON foo1.actor = allactor.id
      WHERE allactor.deathyear IS NULL
      GROUP BY actor) as foo2
     ON actor = allactor.id
WHERE moviecount > 10;

-- QUERY 2 -> Actors whose name starts with “Ja” and who have never participated in any comedy movie
-- TIME TAKEN: ~27 SECONDS
SELECT id, name
FROM (SELECT *
      FROM comedyactor
      UNION ALL
      SELECT *
      FROM noncomedyactor) AS allactor
WHERE NOT EXISTS(SELECT *
                 FROM (SELECT actor, name, genre
                       FROM (SELECT *
                             FROM comedyactor
                             UNION ALL
                             SELECT *
                             FROM noncomedyactor) AS allactor
                                JOIN (SELECT * FROM actedin) AS allmovieactor
                                    ON allactor.id = allmovieactor.actor
                                JOIN (SELECT id, title, year, 'Comedy' AS genre
                                      FROM comedymovie
                                      UNION ALL
                                      SELECT id, title, year, 'Non-Comedy' as genre
                                      FROM noncomedymovie)
                           AS allmovie ON allmovieactor.title = allmovie.id) as foo1
                 WHERE foo1.actor = allactor.id
                   AND foo1.genre = 'Comedy')
  AND name LIKE 'Ja%';


-- USING MATERIALIZED VIEWS ====>

-- QUERY 1 -> Alive actors who have participated in more than 10 movies between 2000 and 2005
-- TIME TAKEN: 367 MS
SELECT id, name, deathyear, moviecount
FROM (SELECT *
      FROM comedyactormaterialized
      UNION ALL
      SELECT *
      FROM noncomedyactormaterialized) AS allactor
         JOIN
     (SELECT actor, count(actor) as moviecount
      FROM (SELECT id, allmovie.title as title, year, actor
            FROM (SELECT id, title, year, 'Comedy' as genre
                  FROM comedymoviematerialized
                  UNION ALL
                  SELECT id, title, year, 'Non-Comedy' as genre
                  FROM noncomedymoviematerialized) as allmovie
                     JOIN (SELECT * FROM actedinmaterialized) AS allmovieactor
                         ON allmovie.id = allmovieactor.title
            WHERE allmovie.year >= 2000
              AND allmovie.year <= 2005) as foo1
               JOIN (SELECT *
                     FROM comedyactormaterialized
                     UNION ALL
                     SELECT *
                     FROM noncomedyactormaterialized) AS allactor ON foo1.actor = allactor.id
      WHERE allactor.deathyear IS NULL
      GROUP BY actor) as foo2
     ON actor = allactor.id
WHERE moviecount > 10;

-- QUERY 2 -> Actors whose name starts with “Ja” and who have never participated in any comedy movie
-- TIME TAKEN: 391 MS
SELECT id, name
FROM (SELECT *
      FROM comedyactormaterialized
      UNION ALL
      SELECT *
      FROM noncomedyactormaterialized) as allactor
WHERE NOT EXISTS(SELECT *
                 FROM (SELECT actor, name, genre
                       FROM (SELECT *
                             FROM comedyactormaterialized
                             UNION ALL
                             SELECT *
                             FROM noncomedyactormaterialized) as allactor
                                JOIN (SELECT * FROM actedinmaterialized) AS allmovieactor
                                     ON allactor.id = allmovieactor.actor
                                JOIN (SELECT id, title, year, 'Comedy' as genre
                                      FROM comedymoviematerialized
                                      UNION ALL
                                      SELECT id, title, year, 'Non-Comedy' as genre
                                      FROM noncomedymoviematerialized) AS allmovie
                                     ON allmovieactor.title = allmovie.id) as foo1
                 WHERE foo1.actor = allactor.id
                   AND foo1.genre = 'Comedy')
  AND name LIKE 'Ja%';
