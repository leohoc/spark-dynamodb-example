package com.lcarvalho.sparkddb.config;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkConfiguration {

    public static JobConf buildJobConf(JavaSparkContext javaSparkContext, final boolean useDefaultAWSCredentials) {

        final JobConf jobConf = new JobConf(javaSparkContext.hadoopConfiguration());
        jobConf.set("dynamodb.servicename", "dynamodb");
        jobConf.set("dynamodb.input.tableName", "Covid19Citation");
        jobConf.set("dynamodb.output.tableName", "Covid19Citation");
        jobConf.set("dynamodb.endpoint", "dynamodb.us-east-1.amazonaws.com");
        jobConf.set("dynamodb.regionid", "us-east-1");
        jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat");
        jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat");

        if (useDefaultAWSCredentials) {
            DefaultAWSCredentialsProviderChain defaultAWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain();
            jobConf.set("dynamodb.awsAccessKeyId", defaultAWSCredentialsProviderChain.getCredentials().getAWSAccessKeyId());
            jobConf.set("dynamodb.awsSecretAccessKey", defaultAWSCredentialsProviderChain.getCredentials().getAWSSecretKey());
        }
        return jobConf;
    }

    public static JavaSparkContext buildSparkContext(String application) throws ClassNotFoundException {
        SparkConf conf = new SparkConf()
                .setAppName(application)
                .registerKryoClasses(new Class<?>[]{
                        Class.forName("org.apache.hadoop.io.Text"),
                        Class.forName("org.apache.hadoop.dynamodb.DynamoDBItemWritable")
                });
        return new JavaSparkContext(conf);
    }

    public static JavaSparkContext buildLocalSparkContext(String application) throws ClassNotFoundException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName(application)
                .registerKryoClasses(new Class<?>[]{
                    Class.forName("org.apache.hadoop.io.Text"),
                    Class.forName("org.apache.hadoop.dynamodb.DynamoDBItemWritable")
                });
        return new JavaSparkContext(conf);
    }
}
