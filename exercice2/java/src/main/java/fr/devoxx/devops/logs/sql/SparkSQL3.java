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

/* Top 3 des familles user agents */
public class SparkSQL3 extends SparkSQL {

    public List<Tuple2<String, Long>> process(JavaRDD<String> rdd, SQLContext sqlContext) {
        JavaRDD<ApacheAccessLog> accessLogs = rdd.map(ApacheAccessLog::parse);
        configure(sqlContext, accessLogs);

        return sqlContext.sql("select agentFamily, count(*) as ct from ApacheAccessLog group by agentFamily order by ct desc limit 3")
                .toJavaRDD()
                .mapToPair(row -> new Tuple2<>(row.getString(0), row.getLong(1)))
                .collect();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + SparkSQL3.class.getName() + " <file>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(SparkSQL3.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            SparkSQL3 sparkSQL3 = new SparkSQL3();
            List<Tuple2<String, Long>> result = sparkSQL3.process(sc.textFile(args[0]), new SQLContext(sc));
            result.forEach(tuple -> System.out.println("->" + tuple._1() + ":" + tuple._2()));
        }
    }
}
