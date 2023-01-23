from pymongo import MongoClient
import pandas
import seaborn
import matplotlib.pyplot

if __name__ == '__main__':
    # connecting to database
    connection = MongoClient()
    database = connection['IMDBMongoDB']
    movies = database['movies']

    # list of all queries
    queries = [[{'$match' : {"genres" : {"$exists" : True}, 'numvotes' : {"$gte" : 10001}}},
                {'$unwind' : "$genres"},
                {'$project' : {"_id" : 1, "genres" : 1, "avgrating" : 1}}],
               [{'$match': {"actors": {"$exists": True}, "genres": {"$exists": True}}},
                {'$unwind': "$actors"},
                {'$group': {'_id': {'movies': "$_id", 'genres': "$genres"}, "NoOfActors": {'$sum': 1}}},
                {'$unwind': "$_id.genres"},
                {'$group': {'_id': "$_id.genres", "average": {'$avg': "$NoOfActors"}}}],
               [{"$group": {'_id': "$startyear", "production": {'$sum': 1}}}]]

    # plotting the first query
    queryOne = movies.aggregate(queries[0])
    dataframeOne = pandas.DataFrame(list(queryOne))
    seaborn.set(rc = {'figure.figsize' : (10, 10)})
    seaborn.boxplot(x = dataframeOne['avgrating'], y = dataframeOne['genres'])
    matplotlib.pyplot.show()

    # plotting the second query
    queryTwo = movies.aggregate(queries[1], allowDiskUse=True)
    dataframeTwo = pandas.DataFrame(list(queryTwo))
    seaborn.set(rc = {'figure.figsize' : (25, 10)})
    seaborn.barplot(x='_id', y='average', data=dataframeTwo)
    matplotlib.pyplot.show()

    # plotting the third query
    queryThree = movies.aggregate(queries[2])
    dataframeThree = pandas.DataFrame(list(queryThree))
    seaborn.set(rc={'figure.figsize': (10, 10)})
    seaborn.lineplot(x='_id', y='production', data=dataframeThree)
    matplotlib.pyplot.show()




