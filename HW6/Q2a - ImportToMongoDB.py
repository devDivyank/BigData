import json

if __name__ == '__main__':
    # opening the extra-data JSON file
    with open('/Users/divyankkulshrestha/PycharmProjects/Intro To Big Data/HW6/extra-data.json') as file:
        cleanJSON = []
        # conversion factors for different currencies (taken from Google Currency conv)
        currencyConversion = {"United States dollar" : 1, "Hong Kong dollar" : 0.13, "Indian rupee" : 0.013, "Egyptian pound" : 0.064,
                              "euro" : 1.11, "Australian dollar" : 0.74, "Russian ruble" : 0.0093, "pound sterling" : 1.32,
                               "Thai baht" : 0.030, "Philippine peso" : 0.019, "1" : 1, "person" : 1, "Czech koruna" : 0.044,}
        allIDs = set()
        for line in file:
            # line is not an empty dictionary
            if len(line) < 1:
                continue
            # dictionary to store useful data
            cleanDict = {}
            currDict = json.loads(line)
            # checking the currency label
            if 'box_office_currencyLabel' in currDict.keys():
                currency = currDict['box_office_currencyLabel']['value']
            # checking the keys and modifying/adding the value if required
            for keyOuter in currDict.keys():
                for keyInner in currDict[keyOuter].keys():
                    if keyInner == 'value':
                        if keyOuter == 'IMDb_ID':
                            # removing 'tt' and casting to integer
                            if int(currDict[keyOuter][keyInner][2:]) in allIDs:
                                break
                            else:
                                allIDs.add(int(currDict[keyOuter][keyInner][2:]))
                                cleanDict['_id'] = int(currDict[keyOuter][keyInner][2:])
                        elif keyOuter == 'cost':
                            # currency conversion
                            cleanDict[keyOuter] = float(currDict[keyOuter][keyInner]) * currencyConversion[currency]
                        elif keyOuter == 'box_office':
                            # currency conversion
                            cleanDict[keyOuter] = float(currDict[keyOuter][keyInner]) * currencyConversion[currency]
                        elif keyOuter == 'box_office_currencyLabel':
                            # skipping currency label
                            continue
                        else:
                            cleanDict[keyOuter] = currDict[keyOuter][keyInner]
            # adding the clean data dictionary to a JSON array
            cleanJSON.append(cleanDict)
    # Exporting the JSON array to a file
    with open('cleanJSON.json', 'w', encoding='utf8') as outputFile:
        json.dump(cleanJSON, outputFile, ensure_ascii=False)

# run the below cammand outside the mongo shell with appropriate db name, collection, filepath etc.

# mongoimport --db IMDBMongoDB --collection boxOfficeInfo --file '/Users/divyankkulshrestha/PycharmProjects/Intro To Big Data/HW6/cleanJSON.json' --drop --jsonArray

