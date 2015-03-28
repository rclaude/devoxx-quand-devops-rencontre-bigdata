package fr.devoxx.devops.logs.spark

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD

/* Répartition des codes http */
case class Spark2(rdd: RDD[String]) {

  def process: Array[(Integer, Long)] = {
    rdd.map(ApacheAccessLog.parse)
      .map(log => (log.code, 1L))
      .reduceByKey((a, b) => a + b)
      .collect
  }
}
