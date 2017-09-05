val businessFile = sc.textFile("/yelp/business/business.csv")
val reviewFile = sc.textFile("/yelp/review/review.csv")
 
val businessMap = businessFile.map(x => x.split("\\^")).map(x => (x(0),x(1),x(2)))
val reviewMap = reviewFile.map(x => x.split("\\^")).map(x=> (x(2),(x(3).toDouble,1))).reduceByKey((a, b) => ((a._1 + b._1), (a._2 + b._2))).map(line =>(line._1,(line._2._1/line._2._2)) ).sortBy(_._2,false)

val businessRDD = businessMap.map { case (business_id, full_address, categories) => ((business_id), (full_address, categories)) }
val reviewRDD = reviewMap.map { case (business_id, stars) => ((business_id), (stars)) }

val outputRDD = businessRDD.join(reviewRDD).map{case ((business_id),((full_address,categories),stars)) => (business_id, full_address,categories,stars) }
outputRDD.sortBy(_._4,false).take(10).foreach(println) 
