package com.lcarvalho.sparkddb.config;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class JobConfiguration {

    public static JobConf build(JavaSparkContext javaSparkContext, String tableName) {
        final JobConf jobConf = new JobConf(javaSparkContext.hadoopConfiguration());
        jobConf.set("dynamodb.servicename", "dynamodb");
        jobConf.set("dynamodb.input.tableName", tableName);
        jobConf.set("dynamodb.output.tableName", tableName);
        jobConf.set("dynamodb.endpoint", "dynamodb.us-east-1.amazonaws.com");
        jobConf.set("dynamodb.regionid", "us-east-1");
        jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat");
        jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat");
        return jobConf;
    }

    public static JobConf localBuild(JavaSparkContext javaSparkContext, String tableName) {
        DefaultAWSCredentialsProviderChain defaultAWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain();
        final JobConf jobConf = new JobConf(javaSparkContext.hadoopConfiguration());
        jobConf.set("dynamodb.servicename", "dynamodb");
        jobConf.set("dynamodb.input.tableName", tableName);
        jobConf.set("dynamodb.output.tableName", tableName);
        jobConf.set("dynamodb.awsAccessKeyId", defaultAWSCredentialsProviderChain.getCredentials().getAWSAccessKeyId());
        jobConf.set("dynamodb.awsSecretAccessKey", defaultAWSCredentialsProviderChain.getCredentials().getAWSSecretKey());
        jobConf.set("dynamodb.endpoint", "dynamodb.us-east-1.amazonaws.com");
        jobConf.set("dynamodb.regionid", "us-east-1");
        jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat");
        jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat");
        return jobConf;
    }

    public static JavaSparkContext buildSparkContext(String application, String tableName) throws ClassNotFoundException {
        SparkConf conf = new SparkConf()
                .setAppName(application + "-" + tableName)
                .registerKryoClasses(new Class<?>[]{
                        Class.forName("org.apache.hadoop.io.Text"),
                        Class.forName("org.apache.hadoop.dynamodb.DynamoDBItemWritable")
                });
        return new JavaSparkContext(conf);
    }

    public static JavaSparkContext buildLocalSparkContext(String application, String tableName) throws ClassNotFoundException {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName(application + "-" + tableName);
        return new JavaSparkContext(conf);
    }
}
