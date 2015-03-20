package fr.devoxx.devops.logs.sql;

import fr.devoxx.devops.logs.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/* Les liens cassés */
public class SparkSQL1 extends SparkSQL {

    public long process(JavaRDD<String> rdd, SQLContext sqlContext) {
        JavaRDD<ApacheAccessLog> accessLogs = rdd.map(ApacheAccessLog::parse);
        configure(sqlContext, accessLogs);

        return sqlContext.sql("select count(distinct(referer)) from ApacheAccessLog where code = 404")
                .toJavaRDD()
                .map(row -> row.getLong(0))
                .first();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + SparkSQL1.class.getName() + " <file>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(SparkSQL1.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            SparkSQL1 sparkSQL1 = new SparkSQL1();
            long result = sparkSQL1.process(sc.textFile(args[0]), new SQLContext(sc));
            System.out.println("->" + result);
        }
    }
}
