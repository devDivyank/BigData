from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import types

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    nameBasicsDF = spark.read.csv("/Users/divyankkulshrestha/Downloads/IMDBDataSet/name.basics.tsv",
                                  sep=r'\t', header=True)
    titleBasicsDF = spark.read.csv("/Users/divyankkulshrestha/Downloads/IMDBDataSet/title.basics.tsv",
                                  sep=r'\t', header=True)
    titlePrincipalsDF = spark.read.csv("/Users/divyankkulshrestha/Downloads/IMDBDataSet/title.principals.tsv",
                                  sep=r'\t', header=True)
    titleRatingsDF = spark.read.csv("/Users/divyankkulshrestha/Downloads/IMDBDataSet/title.ratings.tsv",
                                  sep=r'\t', header=True)


    # QUERY 1: Alive actors whose name starts with “Phi” and did not participate in any movie in 2014
    # TIME TAKEN: ~ 26 seconds
    queryOne = titlePrincipalsDF.join(nameBasicsDF, titlePrincipalsDF.nconst == nameBasicsDF.nconst)\
                                .join(titleBasicsDF, titleBasicsDF.tconst == titlePrincipalsDF.tconst)\
                                .filter((titlePrincipalsDF.category == "actor") & (nameBasicsDF.primaryName.like("Phi%"))
                                        & (nameBasicsDF.deathYear == "\\N") & (titleBasicsDF.titleType == "movie")
                                        & (titleBasicsDF.startYear != "2014"))\
                                .select(titlePrincipalsDF.nconst, nameBasicsDF.primaryName, titleBasicsDF.primaryTitle, titleBasicsDF.startYear)
    queryOne.show(10)


    # QUERY 2: Producers who have produced the most talk shows in 2017 and whose name contains “Gill”.
    # TIME TAKEN: ~ 22 seconds
    titleBasicsDF = titleBasicsDF.withColumn("startYear", titleBasicsDF.startYear.cast(types.IntegerType()))
    titleBasicsDF = titleBasicsDF.withColumn("endYear", titleBasicsDF.endYear.cast(types.IntegerType()))
    titleBasicsDF = titleBasicsDF.withColumn("genres", functions.split(functions.col("genres"), ","))
    queryTwo = titleBasicsDF.join(titlePrincipalsDF, titlePrincipalsDF.tconst == titleBasicsDF.tconst, 'inner') \
                            .join(nameBasicsDF, nameBasicsDF.nconst == titlePrincipalsDF.nconst, "inner") \
                            .filter((titlePrincipalsDF.category == "producer") & (nameBasicsDF.primaryName.like('%Gill%'))
                                    & (titleBasicsDF.startYear == 2017)
                                    & (functions.array_contains(titleBasicsDF.genres, "Talk-Show"))) \
                            .select(nameBasicsDF.nconst, nameBasicsDF.primaryName, titleBasicsDF.tconst, titleBasicsDF.primaryTitle) \
                            .groupBy("nconst", "primaryName").agg(functions.count("tconst").alias("talkShowCount")) \
                            .sort(functions.col("talkShowCount").desc())
    queryTwo.show(10)


    # QUERY 3: Alive producers with the greatest number of long-run titles produced (runtime greater than 120 minutes)
    # TIME TAKEN: ~ 23 seconds
    titleBasicsDF = titleBasicsDF.withColumn("runtimeMinutes", titleBasicsDF.runtimeMinutes.cast(types.IntegerType()))
    queryThree = titleBasicsDF.join(titlePrincipalsDF, titleBasicsDF.tconst == titlePrincipalsDF.tconst) \
                              .join(nameBasicsDF, nameBasicsDF.nconst == titlePrincipalsDF.nconst) \
                              .filter((titleBasicsDF.runtimeMinutes > 120) & (titlePrincipalsDF.category == "producer")
                                      & (nameBasicsDF.deathYear == "\\N")) \
                              .select(titlePrincipalsDF.nconst, nameBasicsDF.primaryName, titleBasicsDF.tconst) \
                              .groupBy("nconst", "primaryName").agg(functions.count("tconst").alias("titleCount")) \
                              .sort(functions.col("titleCount").desc())
    queryThree.show(10)


    # QUERY 4: Alive actors who have portrayed Jesus Christ (look for both words independently)
    # TIME TAKEN: ~25 seconds
    titlePrincipalsDF = titlePrincipalsDF.withColumn("characters", functions.split(functions.col("characters"), ","))
    queryFour = titlePrincipalsDF.join(nameBasicsDF, titlePrincipalsDF.nconst == nameBasicsDF.nconst) \
                                .join(titleBasicsDF, titleBasicsDF.tconst == titlePrincipalsDF.tconst) \
                                .filter((titlePrincipalsDF.category == "actor") & (nameBasicsDF.deathYear == "\\N")
                                        & ((functions.array_contains(titlePrincipalsDF.characters, '["Jesus"]')) |
                                           (functions.array_contains(titlePrincipalsDF.characters, '["Christ"]')))) \
                                .select(nameBasicsDF.nconst, nameBasicsDF.primaryName, nameBasicsDF.deathYear)
    queryFour.show(10)
