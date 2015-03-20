package fr.devoxx.devops.logs.spark;

import fr.devoxx.devops.logs.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/* Top 3 des familles user agents */
public class Spark3 implements Serializable {

    public List<Tuple2<String, Long>> process(JavaRDD<String> rdd) {
        return rdd.map(ApacheAccessLog::parse)
                .mapToPair(log -> new Tuple2<>(log.getAgentFamily(), 1L))
                .reduceByKey(Long::sum)
                .mapToPair(item -> item.swap())
                .sortByKey(false)
                .mapToPair(item -> item.swap())
                .take(3);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + Spark3.class.getName() + " <file>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(Spark3.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Spark3 spark3 = new Spark3();
            List<Tuple2<String, Long>> result = spark3.process(sc.textFile(args[0]));
            result.forEach(tuple -> System.out.println("->" + tuple._1() + ":" + tuple._2()));
        }
    }
}
