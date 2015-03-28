package fr.devoxx.devops.logs.sql

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/* Répartition des codes http */
case class SparkSQL2(rdd: RDD[String] , sqlContext: SQLContext) {

  def process: Array[(Int, Long)] = {
    val dataFrame = sqlContext.createDataFrame(rdd.map(ApacheAccessLog.parse));
    dataFrame.registerTempTable("ApacheAccessLog");

    sqlContext.sql("select code, count(*) from ApacheAccessLog group by code")
      .map(row => (row.getInt(0), row.getLong(1)))
      .collect
  }
}
