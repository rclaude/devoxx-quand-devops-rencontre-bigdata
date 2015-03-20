package fr.devoxx.devops.logs.spark;

import fr.devoxx.devops.logs.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import scala.Tuple4;

import java.io.Serializable;

/* Statistiques sur la taille des requêtes */
public class Spark5 implements Serializable {

    public Tuple4<Long, Double, Double, Double> process(JavaRDD<String> rdd) {
        StatCounter stats = rdd.map(ApacheAccessLog::parse)
                .mapToDouble(log -> Double.valueOf(log.getSize()))
                .stats();
        return new Tuple4<>(stats.count(), stats.min(), stats.mean(), stats.max());
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + Spark5.class.getName() + " <file>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(Spark5.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Spark5 spark5 = new Spark5();
            Tuple4<Long, Double, Double, Double> result = spark5.process(sc.textFile(args[0]));
            System.out.println("->" + result._1() + ", " + result._2() + ", " + result._3() + ", " + result._4());
        }
    }
}
