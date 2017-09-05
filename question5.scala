val business = sc.textFile("/yelp/business/business.csv")
val review = sc.textFile("/yelp/review/review.csv").map(x=>x.split("\\^"))
val texasFilter = business.filter(x=>x.contains("TX")).map(x=>x.split("\\^")).map(x=>(x(0),1)).distinct
val reviewCounter = review.map(x=>(x(2),1)).reduceByKey(_+_)
val Join = texasFilter.join(reviewCounter).collect()
val result = Join.foreach(x=>println(x._1,x._2._2))
