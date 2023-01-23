"""
file: Q1AddingKMeansNormField.py
description: CSCI-620.03 - HMWK8 Q1: adding kmeansnorm field to every document
language: python3
author: Divyank Kulshrestha, dk9924
"""

from pymongo import MongoClient

if __name__ == '__main__':
    # connecting to database
    mongo = MongoClient()
    IMDBMongoDB = mongo["IMDBMongoDB"]

    # creating a collection of movies with numvotes > 10000
    if IMDBMongoDB["moviesover10000votes"] != None:
        IMDBMongoDB.drop_collection("moviesover10000votes")
    IMDBMongoDB["movies"].aggregate([
            {"$match": {"type": {"$eq": "movie"},
                  "numvotes": {"$gt": 10000},
                  "startyear": {"$exists": True},
                  "avgrating": {"$exists": True}}},
            {"$out": "moviesover10000votes"}
    ])
    moviesOver10000Votes = IMDBMongoDB["moviesover10000votes"]

    # GETTING THE MAX AND MIN OF STARTYEAR
    query = [{"$group" : {"_id" : "null", "minstartyear" : {"$min" : "$startyear"}}}]
    minStartYear = moviesOver10000Votes.aggregate(query)
    for document in minStartYear:
        minStartYear = document["minstartyear"]
    query = [{"$group": {"_id": "null", "maxstartyear": {"$max": "$startyear"}}}]
    maxStartYear = moviesOver10000Votes.aggregate(query)
    for document in maxStartYear:
        maxStartYear = document["maxstartyear"]

    # GETTING THE MAX AND MIN OF AVGRATING
    query = [{"$group": {"_id": "null", "minavgrating": {"$min": "$avgrating"}}}]
    minAvgRating = moviesOver10000Votes.aggregate(query)
    for document in minAvgRating:
        minAvgRating = document["minavgrating"]
    query = [{"$group": {"_id": "null", "maxavgrating": {"$max": "$avgrating"}}}]
    maxAvgRating = moviesOver10000Votes.aggregate(query)
    for document in maxAvgRating:
        maxAvgRating = document["maxavgrating"]

    for document in moviesOver10000Votes.find():
        _id = document["_id"]
        startYear = document["startyear"]
        avgRating = document["avgrating"]
        # calculating the normalized values
        normStartYear = (startYear - minStartYear) / (maxStartYear - minStartYear)
        normAvgRating = (avgRating - minAvgRating) / (maxAvgRating - minAvgRating)
        # updating the field in collection
        filter = {"_id" : _id}
        fieldVal = {"$set" : {"kmeansnorm" : [normStartYear, normAvgRating]}}
        moviesOver10000Votes.update_one(filter, fieldVal)

    print("All documents updated with kmeansNorm field.")
