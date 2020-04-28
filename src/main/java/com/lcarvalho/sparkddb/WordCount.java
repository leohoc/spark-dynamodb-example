package com.lcarvalho.sparkddb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.lcarvalho.sparkddb.config.JobConfiguration;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCount {

    private static Logger LOGGER = LogManager.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sparkContext = buildSparkContext();
        JobConf jobConf = JobConfiguration.build(sparkContext);

        JavaPairRDD<Text, DynamoDBItemWritable> prophecies = sparkContext.hadoopRDD(jobConf, DynamoDBInputFormat.class, Text.class, DynamoDBItemWritable.class);

//        LOGGER.info("prophecies count: " + prophecies.count());

        Tuple2<Text, DynamoDBItemWritable> firstProphecy = prophecies.first();
        Map<String, AttributeValue> firstProphecyAttributes = firstProphecy._2.getItem();
        LOGGER.info("prophetCode: " + firstProphecyAttributes.get("prophetCode").getS());
        LOGGER.info("prophecyTimestamp: " + firstProphecyAttributes.get("prophecyTimestamp").getS());
        LOGGER.info("prophecyDate: " + firstProphecyAttributes.get("prophecyDate").getS());
        LOGGER.info("prophecySummary: " + firstProphecyAttributes.get("prophecySummary").getS());
        LOGGER.info("prophecyDescription: " + firstProphecyAttributes.get("prophecyDescription").getS());

        JavaPairRDD<Text, DynamoDBItemWritable> filteredProphecies = prophecies.filter(prophecy -> {
            DynamoDBItemWritable item = prophecy._2();
            Map<String, AttributeValue> attributes = item.getItem();
            return attributes.get("prophecyDate").getS().equals("2020-04-30");
        });

        LOGGER.info("filtered prophecies count: " + filteredProphecies.count());

        JavaRDD propheciesSummaries = filteredProphecies.map(filteredProphecy -> {
            DynamoDBItemWritable item = filteredProphecy._2();
            Map<String, AttributeValue> attributes = item.getItem();
            return attributes.get("prophecySummary").getS();
        });

        JavaRDD<String> words = propheciesSummaries.flatMap(prophecySummary -> Arrays.asList(prophecySummary.toString().split(" ")).iterator());

        LOGGER.info("prophecies summaries word count: " + words.count());

        Map<String, Long> wordCounts = words.countByValue();

        LOGGER.info("prophecies summaries distinct word count: " + wordCounts.size());

        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            LOGGER.info(entry.getKey() + " : " + entry.getValue());
        }
    }

    private static JavaSparkContext buildSparkContext() throws ClassNotFoundException {
        SparkConf conf = new SparkConf().setAppName("wordCounts")
//                .setMaster("local[4]")
                .registerKryoClasses(new Class<?>[]{
                        Class.forName("org.apache.hadoop.io.Text"),
                        Class.forName("org.apache.hadoop.dynamodb.DynamoDBItemWritable")
                });

        return new JavaSparkContext(conf);
    }
}
