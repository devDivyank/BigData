-- NOTE -> Execution times before and after indexing are in 'Q3 - Query Plans' folder.

-- QUERY 1 ->

    SELECT count(*)
    FROM title_actor
    LEFT JOIN actor_title_character ON title_actor.title = actor_title_character.title
                                    AND title_actor.actor = actor_title_character.actor
    WHERE actor_title_character.character IS NULL;


-- QUERY 2 ->

    SELECT member.id, member.name, title.id, title.startyear, title.endyear
    FROM title_actor
             JOIN member ON title_actor.actor = member.id
             JOIN title ON title_actor.title = title.id
    WHERE (member.name LIKE 'Phi%' AND deathYear IS NULL AND
           title.type = 'movie' AND title.startYear != 2014);


-- QUERY 3 ->

    SELECT Final.id, Final.name, Count(Final.id) AS numOfShows
    FROM (SELECT member.id, name, title.startyear, title.endyear
    FROM title_producer JOIN title ON title_producer.title = title.id
                        JOIN member ON title_producer.producer = member.id
                        JOIN title_genre ON title_genre.title = title.id
                        JOIN genre ON title_genre.genre = genre.id
    WHERE (member.name LIKE '%Gill%' AND genre.genre='Talk-Show' AND title.startYear=2017)) AS Final
    GROUP BY Final.id, Final.name
    ORDER BY numOfShows DESC;


-- QUERY 4 ->

    SELECT Final.id, Final.name, count(Final.id) as numOfShows
    FROM (SELECT title.title, title.runtime, member.id, member.name
    FROM title JOIN title_producer ON title_producer.title = title.id
               JOIN member ON member.id = title_producer.producer
    WHERE (title.runtime > 120 AND member.deathYear IS NULL)) as Final
    GROUP BY Final.id, Final.name
    ORDER BY numOfShows DESC;


-- QUERY 5 ->

    SELECT member.id, member.name
    FROM title JOIN title_actor ON title.id = title_actor.title
               JOIN member ON member.id = title_actor.actor
               JOIN actor_title_character ON actor_title_character.actor = member.id AND actor_title_character.title = title.id
               JOIN character ON character.id = actor_title_character.character
    WHERE (character.character = 'Jesus Christ' AND member.deathYear IS NULL)
    GROUP BY member.id;
