"""
file: Q4KMeans.py
description: CSCI-620.03 - HMWK8 Q4: running the K-Means algorithm for different sizes and plotting the sqaured error in each case.
language: python3
author: Divyank Kulshrestha, dk9924
"""

import Q2CreatingCentroidCollection as q2
import Q3AssigningClusters as q3
from pymongo import MongoClient
import seaborn
import matplotlib

def saveCollection(genre, i):
    if "-" in genre:
        genreS = genre.split('-')
        name = '_'.join(genreS) + str(i)
    else:
        name = genre + str(i)
    moviesOver10000Votes.aggregate([{"$match": {}}, {"$out": name}])

def runKMeans(genre: str):
    squaredErrors = {}
    # running on sample sizes 10 to 50, in steps of 5
    for i in range(10, 51, 5):
        # creating a new centroid collection of genre g and size i
        q2.createCentroidsCollection(q2.getSample(i, genre, moviesOver10000Votes), IMDBMongoDB)
        currIteration = 0
        trackConvergence = {}
        converged = False
        # while we keep finding atleast one new centroid
        while currIteration < 100 and not converged:
            # assigning cluster to each movie of genre g
            q3.assignCLusterIDs(genre, moviesOver10000Votes, centroids)
            # updating centroids collection and tracking if all have converged
            updateCentroids(trackConvergence, genre)
            currIteration += 1

            # we stop the iterations if all centroids have converged
            if False in trackConvergence.values():
                continue
            else:
                converged = True
        # storing the squared error for a given sample size, to be plotted later
        squaredErrors[i] = getSquaredError(genre)
        print("Size: " + str(i) + " --> Squared Error: " + str(squaredErrors[i]))
        saveCollection(genre, i)
    return squaredErrors


def getSquaredError(g):
    # getting squared error for a given genre for a given size
    squaredErrorSum = 0
    for document in centroids.find():
        currentCentroid = document
        output = moviesOver10000Votes.aggregate([
            {"$match": {"cluster": currentCentroid["_id"], "genres": {"$eq": g}}},
            {"$project": {"_id": 0, "cluster": 1, "kmeansnorm": 1}},
            {"$group": {"_id": "cluster", "allpoints": {"$push": "$kmeansnorm"}}}
        ])
        # get all points around a centroid and calculate the squared error between them
        allPointsAroundCentroid = None
        for document in output:
            allPointsAroundCentroid = document["allpoints"]
        if allPointsAroundCentroid != None:
            for currentPoint in allPointsAroundCentroid:
                squaredErrorSum += ((currentPoint[0] - currentCentroid["kmeansnorm"][0])**2 + (currentPoint[1] - currentCentroid["kmeansnorm"][1])**2)
    return squaredErrorSum


def updateCentroids(trackConvergence, genre):
    iteration = 1
    for document in centroids.find():
        currentCentroid = document
        newCentroid = q3.findNewCentroid(currentCentroid, moviesOver10000Votes, genre)
        # updating centroid in centroids collection if a new centroid is found
        if newCentroid != None:
            centroids.update_one({"_id": currentCentroid["_id"]},
                                 {"$set": {"kmeansnorm": newCentroid}})

        # if a centroid has converged, we update its status
        if iteration not in trackConvergence.keys():
            trackConvergence[iteration] = False
        if newCentroid == currentCentroid["kmeansnorm"]:
            trackConvergence[iteration] = True
        iteration += 1


if __name__ == '__main__':
    # connecting to database
    mongo = MongoClient()
    IMDBMongoDB = mongo["IMDBMongoDB"]
    moviesOver10000Votes = IMDBMongoDB["moviesover10000votes"]
    centroids = IMDBMongoDB["centroids"]

    # running K-Means for given genres and plotting line graphs
    genres = ["Action", "Horror", "Romance", "Sci-Fi", "Thriller"]
    for genre in genres:
        print("-----> Calculating for '" + str(genre) + "' genre")
        squaredErrors = runKMeans(genre)
        seaborn.set(rc={'figure.figsize': (10, 10)})
        plot = seaborn.lineplot(x=squaredErrors.keys(),
                                y=squaredErrors.values()).set(title=genre, xlabel="Size", ylabel="Sum of Squared Errors")
        matplotlib.pyplot.show()
