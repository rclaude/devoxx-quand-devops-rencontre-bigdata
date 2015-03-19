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



import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering._

val vect = dataAndLabel.map(_._1)

var res = new Array[Double](100);
var x = 0

for(i <- 2 to 10) {
	val clusters = KMeans.train(vect, i, numIterations = 20)
 	res(x) = clusters.computeCost(vect); 
 	x = x+1;
};