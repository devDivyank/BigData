// =========== JOINING COLLECTIONS ON '_ID' ===========
db.movies.aggregate([
    {$lookup : {
        from : "boxOfficeInfo",
        localField : "_id",
        foreignField : "_id",
        as : "extraData"}},
    {$unwind : {'path' : '$extraData',
                'preserveNullAndEmptyArrays' : true }},
    {$addFields : { 'boxofficerevenue' : '$extraData.box_office',
                    'cost' : '$extraData.cost',
                    'distributor' : '$extraData.distributorLabel',
                    'ratingLabel' : '$extraData.MPAA_film_ratingLabel',
                    'titleLabel' : '$extraData.titleLabel'}},
    {$project : {'extraData' : 0}},
    {$out: {db: "IMDBMongoDB", coll: "moviesBoxOffice"}}
]);

// ====> COUNTING THE UPDATES WHEN JOINING ON '_ID' ====> 110689
db.moviesBoxOffice.aggregate([
    {$match : {
        $or : [ {'boxofficerevenue' : {$exists:true}},
                {'titleLabel' : {$exists:true}},
                {'cost' : {$exists:true}},
                {'ratingLabel' : {$exists:true}},
                {'distributor' : {$exists:true}}]
        }},
    {$group : {_id : null, matches : {$sum : 1}}}
]);

