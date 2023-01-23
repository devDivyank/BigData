"""
file: Q2CreatingCentroidCollection.py
description: CSCI-620.03 - HMWK8 Q2: creating a centroid collection with IDs from 1 to k.
language: python3
author: Divyank Kulshrestha, dk9924
"""

from pymongo import MongoClient

def getSample(k: int, g: str, moviesOver10000Votes):
    # getting a sample of size k and genre g
    query = [{"$match": {"genres" : g}},
             {"$sample" : {"size" : k}},
             {"$project" : {"_id" : 1, "kmeansnorm" : 1}}]
    return moviesOver10000Votes.aggregate(query)

def createCentroidsCollection(sample, IMDBMongoDB):
    # dropping the previous centroid collection, if one exists
    if IMDBMongoDB["centroids"] != None:
        IMDBMongoDB.drop_collection("centroids")
    # creating the documents with IDs from 1 to k where k is the size of sample
    IMDBMongoDB.create_collection("centroids")
    newID = 1
    allDocs = []
    for document in sample:
        allDocs.append({"_id" : newID, "kmeansnorm" : document["kmeansnorm"]})
        newID += 1
    # adding all the documents into the new centroid collection
    IMDBMongoDB["centroids"].insert_many(allDocs)

if __name__ == '__main__':
    # connecting to database
    mongo = MongoClient()
    IMDBMongoDB = mongo["IMDBMongoDB"]
    moviesOver10000Votes = IMDBMongoDB["moviesover10000votes"]

    k = int(input("Enter the sample size: "))
    g = input("Enter the genre: ")

    # getting the sample and then creating the centroid collection
    sizeKSample = getSample(k, g, moviesOver10000Votes)
    createCentroidsCollection(sizeKSample, IMDBMongoDB)

    print("Centroids collection created.")