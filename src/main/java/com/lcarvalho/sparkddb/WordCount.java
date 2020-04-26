package com.lcarvalho.sparkddb;

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
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {

    private static Logger LOGGER = LogManager.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JobConf jobConf = JobConfiguration.build(sparkContext);

        JavaPairRDD javaPairRDD = sparkContext.hadoopRDD(jobConf, DynamoDBInputFormat.class, Text.class, DynamoDBItemWritable.class);

        LOGGER.info("table itens count: " + javaPairRDD.count());
    }
}
