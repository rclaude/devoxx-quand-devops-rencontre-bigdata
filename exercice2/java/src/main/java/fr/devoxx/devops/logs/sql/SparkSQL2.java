package fr.devoxx.devops.logs.sql;

import fr.devoxx.devops.logs.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/* Répartition des codes http */
public class SparkSQL2 extends SparkSQL {

    public List<Tuple2<Integer, Long>> process(JavaRDD<String> rdd, SQLContext sqlContext) {
        JavaRDD<ApacheAccessLog> accessLogs = rdd.map(ApacheAccessLog::parse);
        configure(sqlContext, accessLogs);

        return sqlContext.sql("select code, count(*) from ApacheAccessLog group by code")
                .toJavaRDD()
                .mapToPair(row -> new Tuple2<>(row.getInt(0), row.getLong(1)))
                .collect();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + SparkSQL2.class.getName() + " <file>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(SparkSQL2.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            SparkSQL2 sparkSQL2 = new SparkSQL2();
            List<Tuple2<Integer, Long>> result = sparkSQL2.process(sc.textFile(args[0]), new SQLContext(sc));
            result.forEach(tuple -> System.out.println("->" + tuple._1() + ":" + tuple._2()));
        }
    }
}
