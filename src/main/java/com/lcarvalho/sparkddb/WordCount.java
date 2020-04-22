package com.lcarvalho.sparkddb;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

//        JavaRDD<String> lines = sparkContext.textFile("in/word_count.text");
//        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//
//        Map<String, Long> wordCounts = words.countByValue();
//
//        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
//            System.out.println(entry.getKey() + " : " + entry.getValue());
//        }


        JobConf jobConf = new JobConf(sparkContext.hadoopConfiguration());
        jobConf.set("dynamodb.servicename", "dynamodb");
        jobConf.set("dynamodb.input.tableName", "Prophecy");
        jobConf.set("dynamodb.endpoint", "http://localhost:8000/");
        jobConf.set("dynamodb.regionid", "us-west-2");
        jobConf.set("dynamodb.throughput.read", "1");
        jobConf.set("dynamodb.throughput.read.percent", "1");
        jobConf.set("dynamodb.version", "2011-12-05");
        jobConf.set("dynamodb.awsAccessKeyId", "localAccessKey");
        jobConf.set("dynamodb.awsSecretAccessKey", "secretAccessKey");

        jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat");
        jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat");

        JavaPairRDD orders = sparkContext.hadoopRDD(jobConf, DynamoDBInputFormat.class, Text.class, DynamoDBItemWritable.class);

        orders.count();
    }
}
