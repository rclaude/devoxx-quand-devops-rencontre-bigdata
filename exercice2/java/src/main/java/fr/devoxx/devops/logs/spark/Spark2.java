package fr.devoxx.devops.logs.spark;

import fr.devoxx.devops.logs.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/* Répartition des codes http */
public class Spark2 implements Serializable {

    public List<Tuple2<Integer, Long>> process(JavaRDD<String> rdd) {
        return rdd.map(ApacheAccessLog::parse)
                .mapToPair(log -> new Tuple2<>(log.getCode(), 1L))
                .reduceByKey(Long::sum)
                .collect();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + Spark2.class.getName() + " <file>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(Spark2.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Spark2 spark2 = new Spark2();
            List<Tuple2<Integer, Long>> result = spark2.process(sc.textFile(args[0]));
            result.forEach(tuple -> System.out.println("->" + tuple._1() + ":" + tuple._2()));
        }
    }
}
