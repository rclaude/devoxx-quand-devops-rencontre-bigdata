package fr.devoxx.devops.logs.streaming

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.streaming.StreamingContext

/* Compter le nombre de code http à 404. */
case class SparkStreaming1(hostname: String, port: Int, sc: StreamingContext) {

  def process: Unit = {
    sc.socketTextStream(hostname, port)
      .map(ApacheAccessLog.parse)
      .map(log => log.code)
      .filter(code => code ==404)
      .count()
      .print()
  }

}
