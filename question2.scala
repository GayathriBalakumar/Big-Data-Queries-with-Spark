val userFile = sc.textFile("/yelp/user/user.csv")
val reviewFile = sc.textFile("/yelp/review/review.csv")
val userName =readLine("Enter the name of the user")
val userMap = userFile.map(x => x.split("\\^")).filter(x => x(1).contains(userName)).map(x => (x(0),x(1)))
val reviewMap = reviewFile.map(x => x.split("\\^")).map(x=> (x(1),(x(3).toDouble,1))).reduceByKey((a, b) => ((a._1 + b._1), (a._2 + b._2))).map(x=>(x._1,(x._2._1/x._2._2)) ).sortBy(_._2,false)
val reviewRDD = reviewMap.map { case (userID, stars) => ((userID), stars) }
val userRDD = userMap.map{ case(userID, userName) => ((userID),userName)}
val outputRDD = userRDD.join(reviewRDD).map { case((userID),(userName, stars)) => (userID, userName, stars)}	
outputRDD.collect().foreach(println)
