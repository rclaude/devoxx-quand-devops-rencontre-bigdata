package fr.devoxx.devops.logs.spark;

import fr.devoxx.devops.logs.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/* Top 3 des plages d'IP */
public class Spark4 implements Serializable {

    public List<Tuple2<String, Long>> process(JavaRDD<String> rdd) {
        return rdd.map(ApacheAccessLog::parse)
                .mapToPair(log -> new Tuple2<>(log.getIPRange(), 1L))
                .reduceByKey(Long::sum)
                .top(3, new ValueComparator<>(Comparator.<Long>naturalOrder()));
    }

    private static class ValueComparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable {
        private Comparator<V> comparator;

        public ValueComparator(Comparator<V> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
            return comparator.compare(o1._2(), o2._2());
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + Spark4.class.getName() + " <file>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(Spark4.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Spark4 spark4 = new Spark4();
            List<Tuple2<String, Long>> result = spark4.process(sc.textFile(args[0]));
            result.forEach(tuple -> System.out.println("->" + tuple._1() + ":" + tuple._2()));
        }
    }
}
