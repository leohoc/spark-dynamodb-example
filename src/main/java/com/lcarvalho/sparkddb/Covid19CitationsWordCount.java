package com.lcarvalho.sparkddb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.lcarvalho.sparkddb.config.SparkConfiguration;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class Covid19CitationsWordCount {

    private static final String APP_NAME = "Covid19CitationsWordCount";
    private static Logger LOGGER = LogManager.getLogger(Covid19CitationsWordCount.class);

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        // Building the Spark session
        JavaSparkContext sparkContext = SparkConfiguration.buildSparkContext(APP_NAME);
        JobConf jobConf = SparkConfiguration.buildJobConf(sparkContext, Boolean.FALSE);

        // Building a RDD pointing to the DynamoDB table
        JavaPairRDD<Text, DynamoDBItemWritable> citations = sparkContext.hadoopRDD(jobConf, DynamoDBInputFormat.class, Text.class, DynamoDBItemWritable.class);
        LOGGER.info("Citations count: " + citations.count());

        // Filtering citations published in 2020
        JavaPairRDD<Text, DynamoDBItemWritable> filteredCitations = citations.filter(citation -> {
            DynamoDBItemWritable item = citation._2();
            Map<String, AttributeValue> attributes = item.getItem();
            return attributes.get("publishedYear") == null ? Boolean.FALSE : attributes.get("publishedYear").getN().equals("2020");
        });
        LOGGER.info("Filtered citations count: " + filteredCitations.count());

        // Building a RDD with the citations titles
        JavaRDD citationsTitles = filteredCitations.map(citation -> {
            DynamoDBItemWritable item = citation._2();
            Map<String, AttributeValue> attributes = item.getItem();
            return attributes.get("title").getS();
        });

        // Building a RDD with the citations titles words
        JavaRDD<String> citationsTitlesWords = citationsTitles.flatMap(citationTitle -> Arrays.asList(citationTitle.toString().split(" ")).iterator());
        LOGGER.info("Citations titles word count: " + citationsTitlesWords.count());

        // Counting the number of times each word was used
        Map<String, Long> wordCounts = citationsTitlesWords.countByValue();
        LOGGER.info("Citations titles distinct word count: " + wordCounts.size());
    }
}
