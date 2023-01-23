CREATE TABLE L1 AS
    (SELECT * FROM
            (SELECT actor AS actor1, count(movie) AS MovieCount
            FROM popular_movie_actors GROUP BY actor1) as foo1
    WHERE foo1.MovieCount >= 5);