CREATE TABLE L3 AS
(SELECT foo3.actor1, foo3.actor2, foo3.actor3, count FROM
(SELECT foo2.actor1, foo2.actor2, foo2.actor3, count(movie) AS count FROM
(SELECT * FROM
(SELECT p1.movie, p1.actor as actor1, p2.actor as actor2, p3.actor as actor3
    FROM popular_movie_actors p1
    JOIN popular_movie_actors p2 ON p1.movie = p2.movie AND p1.actor > p2.actor
    JOIN popular_movie_actors p3 ON p2.movie = p3.movie AND p2.actor > p3.actor) AS foo1
    WHERE EXISTS (SELECT * FROM l2 WHERE actor1 = foo1.actor1 AND actor2 = foo1.actor2)
    AND EXISTS (SELECT * FROM l2 WHERE actor1 = foo1.actor2 AND actor2 = foo1.actor3)
    AND EXISTS (SELECT * FROM l2 WHERE actor1 = foo1.actor1 AND actor2 = foo1.actor3)) AS foo2
GROUP BY actor1, actor2, actor3) AS foo3
WHERE count >= 5);

