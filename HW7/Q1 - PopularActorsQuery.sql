CREATE TABLE Popular_Movie_Actors AS
    SELECT title_actor.title AS movie, title_actor.actor AS actor FROM title
                                        JOIN title_actor ON title.id = title_actor.title
            WHERE title.type = 'movie' AND title.avgrating > 5;

