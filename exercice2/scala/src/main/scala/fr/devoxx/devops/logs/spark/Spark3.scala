package fr.devoxx.devops.logs.spark

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD

/* Top 3 des familles user agents */
case class Spark3(rdd: RDD[String]) {

  def process: Array[(String, Long)] = {
    rdd.map(ApacheAccessLog.parse)
      .map(log => (log.agentFamily, 1L))
      .reduceByKey((a, b) => a + b)
      .map(item => item.swap)
      .sortByKey(false)
      .map(item => item.swap)
      .take(3)
  }
}
