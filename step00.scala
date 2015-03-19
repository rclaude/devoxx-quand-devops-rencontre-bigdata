var data = sc.textFile("/vagrant/data/kddcup.data")
//data.cache()
data.count()

data.take(1)
//res11: Array[String] = Array(0,tcp,http,SF,215,45076,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,normal.)

val l10 = data.take(10).map(_.split(','))

// quels sont les protocols
val protocols = data.map(l => l.split(",")(2))
protocols.distinct().collect()

//compter un protocol
val httpCount = data.filter(l => l.split(",")(2).contains("http")).count()

//compter tous les protocols
val protocolPairs = protocols.map(s => (s,1))
val counts = protocolPairs.reduceByKey((a,b) => a+b)
counts.collect()

// tri
val protocolsCountsOrdered = counts.sortByKey()
protocolsCountsOrdered.collect()

// tri sur la valeur serait mieux
val protocolsCountsOrdered = counts.map(_.swap).sortByKey()
protocolsCountsOrdered.collect()

// finalement ce qu'on veut c'est le top10
val top = counts.map(_.swap).top(10)
top.foreach(println)





