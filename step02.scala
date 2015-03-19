///////////////
////Clustering

import scala.collection.mutable.ArrayBuffer

var data = sc.textFile("/vagrant/data/kddcup.data")

val dataAndLabel = data.map { l =>
 val buffer = ArrayBuffer[String]()
 buffer.appendAll(l.split(","))
 buffer.remove(1,3)
 val label = buffer.remove(buffer.length-1)
 val vector = buffer.map(_.toDouble).toArray
 (vector,label)
}

dataAndLabel.take(1)
//Array[(Array[Double], String)] = Array((Array(0.0, 215.0, 45076.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),normal.))


