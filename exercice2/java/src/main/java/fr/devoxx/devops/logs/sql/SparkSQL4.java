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

/* Top 3 des plages d'IP */
public class SparkSQL4 extends SparkSQL {

    public List<Tuple2<String, Long>> process(JavaRDD<String> rdd, SQLContext sqlContext) {
        JavaRDD<ApacheAccessLog> accessLogs = rdd.map(ApacheAccessLog::parse);
        configure(sqlContext, accessLogs);

        return sqlContext.sql("select ipRange, count(*) as ct from ApacheAccessLog group by ipRange order by ct desc limit 3")
                .toJavaRDD()
                .mapToPair(row -> new Tuple2<>(row.getString(0), row.getLong(1)))
                .collect();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + SparkSQL4.class.getName() + " <file>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(SparkSQL4.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            SparkSQL4 sparkSQL4 = new SparkSQL4();
            List<Tuple2<String, Long>> result = sparkSQL4.process(sc.textFile(args[0]), new SQLContext(sc));
            result.forEach(tuple -> System.out.println("->" + tuple._1() + ":" + tuple._2()));
        }
    }
}
