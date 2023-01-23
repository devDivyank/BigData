CREATE TABLE L2 AS (SELECT * FROM
(SELECT actor1, actor2, count(movie) AS count FROM
(SELECT p1.movie as movie, p1.actor as actor1, p2.actor AS actor2
        FROM popular_movie_actors p1
        JOIN popular_movie_actors p2 ON p1.movie = p2.movie WHERE p1.actor > p2.actor) AS foo1
        WHERE EXISTS (SELECT * FROM l1 WHERE actor1 = foo1.actor1)
        AND EXISTS (SELECT * FROM l1 WHERE actor1 = foo1.actor2)
        GROUP BY actor1, actor2) AS foo2
WHERE count >= 5);

