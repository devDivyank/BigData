"""
file: Q3AssigningClusters.py
description: CSCI-620.03 - HMWK8 Q3: running one step of K-Means by assigning a cluster ID to each movie
                                                    of the given genre and updating the centroids, if a new one is found
language: python3
author: Divyank Kulshrestha, dk9924
"""

import math
from pymongo import MongoClient


def getNearestCluster(currentPoint, centroids):
    # finding the closest centroid point to a given point
    closestCluster = None
    minDistance = float("inf")
    for document in centroids.find():
        clusterPoint = document["kmeansnorm"]
        # choosing the point with lowest euclidean distance
        euclideanDistance = math.sqrt(((currentPoint[0] - clusterPoint[0]) ** 2) +
                                      ((currentPoint[1] - clusterPoint[1]) ** 2))
        if euclideanDistance < minDistance:
            closestCluster = document
            minDistance = euclideanDistance
    return closestCluster


def assignCLusterIDs(g, moviesOver10000Votes, centroids):
    # getting all movies of genre g
    sample = moviesOver10000Votes.aggregate([
        {"$match": {"genres": g}},
        {"$project": {"_id": 1, "kmeansnorm": 1}}
    ])
    # assigning the nearest cluster's ID as cluster for each movie selected above
    for document in sample:
        currentPoint = document["kmeansnorm"]
        nearestCluster = getNearestCluster(currentPoint, centroids)
        moviesOver10000Votes.update_one({"_id": document["_id"]},
                                        {"$set": {"cluster": nearestCluster["_id"]}})


def updateCentroids():
    # updating the centroids in the centroids collection if a new centroid is found
    for document in centroids.find():
        currentCentroid = document
        newCentroid = findNewCentroid(currentCentroid, moviesOver10000Votes, g)
        if newCentroid != None:
            centroids.update_one({"_id": currentCentroid["_id"]},
                                 {"$set": {"kmeansnorm": newCentroid}})


def findNewCentroid(currentCentroid, moviesOver10000Votes, g):
    # getting kmeansnorm field of all movies of genre g in one cluster, as an array
    output = moviesOver10000Votes.aggregate([
        {"$match": {"cluster": currentCentroid["_id"], "genres": {"$eq": g}}},
        {"$project": {"_id": 0, "cluster": 1, "kmeansnorm": 1}},
        {"$group": {"_id": "cluster", "allpoints": {"$push": "$kmeansnorm"}}}
    ])
    allPointsAroundCentroid = None
    for document in output:
        allPointsAroundCentroid = document["allpoints"]

    # if there is/are point(s) around a centroid, we return the new centroid as the average of kmeansnorm of all such points
    if allPointsAroundCentroid != None and len(allPointsAroundCentroid) > 0:
        newCentroid = []
        xVal = 0
        yVal = 0
        for point in allPointsAroundCentroid:
            xVal += point[0]
            yVal += point[1]
        newCentroid.append(xVal / len(allPointsAroundCentroid))
        newCentroid.append(yVal / len(allPointsAroundCentroid))
        return newCentroid
    return None


if __name__ == '__main__':
    # connecting to database
    mongo = MongoClient()
    IMDBMongoDB = mongo["IMDBMongoDB"]
    moviesOver10000Votes = IMDBMongoDB["moviesover10000votes"]
    centroids = IMDBMongoDB["centroids"]

    g = input("Enter the genre: ")
    assignCLusterIDs(g, moviesOver10000Votes, centroids)
    print("Assigned clusterID.")
    updateCentroids()
    print("Centroids collection updated to new centroids.")
