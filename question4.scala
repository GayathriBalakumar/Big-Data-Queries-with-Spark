val review = sc.textFile("/yelp/review/review.csv").map(x=>x.split("\\^"))
val user=sc.textFile("/yelp/user/user.csv").map(line=>line.split("\\^"))
val userMap=user.map(line=>(line(0),line(1).toString))
val count=review.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b).distinct
val join=userMap.join(count).distinct.collect()
val Topten=join.sortWith(_._2._2>_._2._2).take(10)
println("")
println("user_id        name")
Topten.foreach(line=>println(line._1,line._2._1))
