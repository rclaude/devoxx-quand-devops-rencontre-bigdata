package fr.devoxx.devops.logs.sql;

import fr.devoxx.devops.logs.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple4;

import java.io.Serializable;

import static java.lang.Double.valueOf;

/* Statistiques sur la taille des requêtes */
public class SparkSQL5 extends SparkSQL {

    public Tuple4<Long, Long, Double, Long> process(JavaRDD<String> rdd, SQLContext sqlContext) {
        JavaRDD<ApacheAccessLog> accessLogs = rdd.map(ApacheAccessLog::parse);
        configure(sqlContext, accessLogs);

        return sqlContext.sql("select count(size), min(size), avg(size), max(size) from ApacheAccessLog")
                .toJavaRDD()
                .map(row -> new Tuple4<>(
                        row.getLong(0),
                        row.getLong(1),
                        valueOf(row.getDouble(2)),
                        row.getLong(3)
                ))
                .first();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + SparkSQL5.class.getName() + " <file>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(SparkSQL5.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            SparkSQL5 sparkSQL5 = new SparkSQL5();
            Tuple4<Long, Long, Double, Long> result = sparkSQL5.process(sc.textFile(args[0]), new SQLContext(sc));
            System.out.println("->" + result._1() + ", " + result._2() + ", " + result._3() + ", " + result._4());
        }
    }
}
