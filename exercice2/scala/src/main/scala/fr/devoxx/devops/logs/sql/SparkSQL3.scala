package fr.devoxx.devops.logs.sql

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/* Top 3 des familles user agents */
case class SparkSQL3(rdd: RDD[String] , sqlContext: SQLContext) {

  def process: Array[(String, Long)] = {
    val dataFrame = sqlContext.createDataFrame(rdd.map(ApacheAccessLog.parse));
    dataFrame.registerTempTable("ApacheAccessLog");

    sqlContext.sql("select agentFamily, count(*) as ct from ApacheAccessLog group by agentFamily order by ct desc limit 3")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect
  }
}
