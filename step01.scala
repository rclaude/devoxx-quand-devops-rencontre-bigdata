var data = sc.textFile("/vagrant/data/kddcup.data")

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._

case class Protocol(tcpudp:String,port:String,hit:Integer)

val hits = data.map(_split(",")).map(l => Protocol(l(1),l(2),1))

hits.registerAsTable("hits")

val counts = sql("SELECT port, COUNT(*) c FROM hits GROUP BY port ORDER BY c DESC LIMIT 10")
counts.foreach(println)



