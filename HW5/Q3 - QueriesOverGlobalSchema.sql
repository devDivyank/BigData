-- QUERY 1 -> Alive actors who have participated in more than 10 movies between 2000 and 2005

SELECT id, name, birthyear, deathyear, moviecount
FROM all_actor
         JOIN
     (SELECT actor, count(actor) as moviecount
      FROM (SELECT id, all_movie.title as title, year, actor
            FROM all_movie
                     JOIN all_movie_actor ON all_movie.id = all_movie_actor.title
            WHERE all_movie.year >= 2000
              AND all_movie.year <= 2005) as foo1
               JOIN all_actor ON foo1.actor = all_actor.id
      WHERE all_actor.deathyear IS NULL
      GROUP BY actor) as foo2
     ON actor = all_actor.id
WHERE moviecount > 10;

-- QUERY 2 -> Actors whose name starts with “Ja” and who have never participated in any comedy movie

SELECT *
FROM all_actor
WHERE NOT EXISTS(SELECT *
                 FROM (SELECT actor, name, genre
                       FROM all_actor
                                JOIN all_movie_actor ON all_actor.id = all_movie_actor.actor
                                JOIN all_movie ON all_movie_actor.title = all_movie.id) as foo1
                 WHERE foo1.actor = all_actor.id
                   AND foo1.genre = 'Comedy')
  AND name LIKE 'Ja%';
