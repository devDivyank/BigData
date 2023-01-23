# If there is no _ID field, we can insert the data by matching 'titlelabel' in extra-data with
# 'title' and 'originaltitle' in database.
# However, the issue with matching on movie title is that there might be multiple movies with
# the same name. This could potentially cause the following issue:
#          ==> Same data is updated in multiple documents where the title of the movie is same.
#              Thus, we will detect more updates than what the actual number of updates should've been.
#              For instance, we have multiple fields with the name(s)
#                       --- 'A Good Day to Die Hard'
#                       --- 'To Catch A Thief'
#                       --- 'To All a Goodnight'
#                       and more...
#              So when we match on 'titleLabel', data in extra-data is updated in all the
#              documents with the title above. In total, we have 29750 titles in extra-data that
#              have multiple matches in the database.
#          ==> If we update the '_id' field when the 'titleLabel' matches with title/originaltitle in multiple
#              documents, we could get a duplicate key error, since the new '_id' from extra-data will already exist
#              in document(s) that were updated before. That is why, we skip updating the '_id' in the program below.

# Successful updates made when joining on 'titlelabel' = 242604
# Successful updates made when joining on '_id' = 110689
# ======> EXTRA UPDATES MADE = 131915 <=======

# CREATING A COPY OF MOVIE COLLECTION TO PERFORM OPERATIONS ON ===>
#               db.movies.aggregate([{$match: {}}, {$out: "moviesTemp"}])
# CREATING INDEXES TO SPEED UP THE QUERIES ===>
#               db.moviesTemp.createIndex({"title" : 1})
#               db.moviesTemp.createIndex({"originalTitle" : 1})
# DROPPING THE TEMP TABLE AFTER WE ARE DONE ===>
#               db.getCollection("moviesTemp").drop();

import json
from pymongo import MongoClient

if __name__ == '__main__':
    # connecting to database
    connection = MongoClient()
    database = connection['IMDBMongoDB']
    moviesTemp = database['moviesTemp']
    cleanJSON = None
    # opening clean-data JSON file
    with open('/Users/divyankkulshrestha/PycharmProjects/Intro To Big Data/HW6/cleanJSON.json') as file:
        for line in file:
            cleanJSON = json.loads(line)
    updateCount = 0
    movieSeen = set()
    # for each movie-dictionary in clean data
    for dataDict in cleanJSON:
        # skipping the movie if there is no titleLabel
        if 'titleLabel' not in dataDict.keys():
            continue
        # storing the values to be updated
        data = {}
        for key in dataDict.keys():
            if key == '_id' or key == 'titleLabel':
                continue
            else:
                data[key] = dataDict[key]
        movieTitle = dataDict['titleLabel']
        # skip the movie if already seen
        if movieTitle in movieSeen:
            continue
        else:
            movieSeen.add(movieTitle)
        # matching 'titlelabel' with 'title' or 'originalTitle' and counting the updates
        queryOne = moviesTemp.update_many(
            {'$or' : [{'title' : movieTitle},
                      {'originalTitle': movieTitle}]},
            {'$set' : data})
        updateCount += queryOne.modified_count
    print("Total updates made : " + str(updateCount))

# COUNT OF THE SUCCESSFUL UPDATES IS PRINTED AT THE END OF EXECUTION.
