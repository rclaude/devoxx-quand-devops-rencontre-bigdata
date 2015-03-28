package fr.devoxx.devops.logs.sql

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/* Statistiques sur la taille des requêtes */
case class SparkSQL5(rdd: RDD[String] , sqlContext: SQLContext) {

  def process: (Long, Long, Double, Long) = {
    val dataFrame = sqlContext.createDataFrame(rdd.map(ApacheAccessLog.parse));
    dataFrame.registerTempTable("ApacheAccessLog");

    sqlContext.sql("select count(size), min(size), avg(size), max(size) from ApacheAccessLog")
      .map(row => (row.getLong(0), row.getLong(1), row.getDouble(2), row.getLong(3)))
      .first
  }
}
