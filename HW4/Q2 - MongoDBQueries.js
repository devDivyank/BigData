// QUERY RUNTIMES ARE IN PART-4 OF THE ASSIGNMENT

// QUERY 1 - Alive actors whose name starts with “Phi” and did not participate in any movie in 2014.
db.movies.aggregate([{$match : {"startyear" : {"$ne": 2014}}},
             {$lookup : {
                from : "members",
                localField : "actors.actor",
                foreignField : "_id",
                as : "actingcast"
             }},
             {$unwind : "$actingcast"},
             {$match : {"actingcast.name" : {"$regex" : "^Phi*"}, "actingcast.deathYear" : {"$exists" : false}}},
             {$project : {"actingcast.name" : 1}}])

// QUERY 2 - Producers who have produced more than 50 talk shows in 2017 and whose name contain “Gill”.
db.movies.aggregate([
            {$match : {"genres" : {"$eq" : "Talk-Show"}, "startyear" : {"$eq" : 2017}}},
            {$lookup: {
                from: "members",
                localField: "producers",
                foreignField: "_id",
                as: "producingcast"
                }},
            {$unwind: "$producingcast"},
            {$match : {"producingcast.name" : {"$regex" : "\S*Gill\S*"}}},
            {$group : { _id : "$producingcast.name", count : {$sum : 1}}},
            {$match : {"count" : { "$gte" : 50}}}])

// QUERY 3 - Average runtime for movies that were written by members whose names contain “Bhardwaj” and are still alive.
db.movies.aggregate([
            {$unwind : "$writers"},
            {$group : {_id : "$writers", runtimes: {$push : "$runtime"}}},
            {$lookup : {
                from : "members",
                localField : "_id",
                foreignField : "_id",
                as : "writingcast"
            }},
            {$match : {"writingcast.name" : /Bhardwaj/, "writingcast.deathyear" : {$exists : false}}},
            {$unwind : "$runtimes"},
            {$group : {_id : null, avgruntime: {$avg : "$runtimes"}}},
            {$project : {_id : 0}}])

// QUERY 4 - Alive producers with the greatest number of long-run movies produced (runtime greater than 120 minutes)
db.movies.aggregate([
            {$match : {"runtime" : {"$gte" : 120}, "type" : {"$eq" : "movie"}}},
            {$lookup : {
                from : "members",
                localField : "producers",
                foreignField : "_id",
                as : "producingcast"}},
            {$match : {"producingcast.deathyear" : {$exists : false}}},
            {$unwind : "$producingcast"},
            {$group : {_id : "$producingcast.name", numTitles : {$sum : 1}}}])

// QUERY 5 - Sci-Fi movies directed by James Cameron and acted in by Sigourney Weaver.

db.movies.aggregate([
            {$match : {"genres" : {"$eq" : "Sci-Fi"}}},
            {$unwind : "$directors"},
            {$lookup : {
                from : "members",
                localField : "directors",
                foreignField : "_id",
                as : "directingcast"}},
            {$match : {"directingcast.name" : {"$eq" : "James Cameron"}}},
            {$unwind : "$actors"},
            {$lookup : {
                from : "members",
                localField : "actors.actor",
                foreignField : "_id",
                as : "actingcast"}},
            {$match : {"actingcast.name" : {"$eq" : "Sigourney Weaver"}}},
            {$project : {"_id" : 1, "actingcast.name" : 1, "directingcast.name" : 1,
                                            "originaltitle" :1, "actors.roles" :1}}])
