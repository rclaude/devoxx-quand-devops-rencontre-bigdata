package fr.devoxx.devops.logs.streaming;

import fr.devoxx.devops.logs.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

/**
 * Compter le nombre de code http à 404.
 */
public class SparkStreaming1 implements Serializable {

    public void process(String hostname, int port, JavaStreamingContext sc) {
        sc.socketTextStream(hostname, port)
                .map(ApacheAccessLog::parse)
                .map(ApacheAccessLog::getCode)
                .filter(code -> code == 404)
                .count()
                .print();
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: " + SparkStreaming1.class.getName() + " <hostname> <port>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName(SparkStreaming1.class.getName());
        try (JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1))) {
            SparkStreaming1 sparkStreaming1 = new SparkStreaming1();
            sparkStreaming1.process(args[0], Integer.parseInt(args[1]), sc);
        }
    }
}
