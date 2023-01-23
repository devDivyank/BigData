// ADDED INDEXING ONLY FOR ATTRIBUTES THAT ARE BEING USED IN ONE OR MORE OPERATIONS
//  (FOR INSTANCE: ACCESSING A ATTRIBUTE OR SEARCHING FOR A VALUE ETC.)

// SCRIPTS FOR CREATING INDEXES ARE GIVEN BELOW:

db.movies.createIndex({"startyear" : 1})
db.movies.createIndex({"genres" : 1})
db.movies.createIndex({"runtime" : 1})
db.movies.createIndex({"type" : 1})
db.movies.createIndex({"actors.actor" : 1})
db.movies.createIndex({"producers" : 1})
db.movies.createIndex({"writers" : 1})
db.movies.createIndex({"directors" : 1})

db.members.createIndex({"name" : 1})

// INDEX FOR '_id' IS CREATED AUTOMATICALLY.
//  CREATING AN INDEX ELIMINATES THE NEED TO SCAN THE WHOLE COLLECTION FOR LOOKING UP A VALUE AND THUS
//  IMPROVES THE EXECUTION TIMES.

// QUERY RUNTIMES BEFORE INDEXING:
// ==> QUERY 1 : 14m 6s 580ms
// ==> QUERY 2 : 14s 758ms
// ==> QUERY 3 : 1m 6s 663ms
// ==> QUERY 4 : 39s 361ms
// ==> QUERY 5 : 38s 429ms

// QUERY RUNTIMES AFTER INDEXING:
// ==> QUERY 1 : 9m 5s 671ms
// ==> QUERY 2 : 8s 299ms
// ==> QUERY 3 : 52s 592ms
// ==> QUERY 4 : 37s 744ms
// ==> QUERY 5 : 28s 829ms
