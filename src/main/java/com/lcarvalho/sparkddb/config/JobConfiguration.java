package com.lcarvalho.sparkddb.config;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaSparkContext;

public class JobConfiguration {

    public static JobConf build(JavaSparkContext javaSparkContext) {
        DefaultAWSCredentialsProviderChain defaultAWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain();
        final JobConf jobConf = new JobConf(javaSparkContext.hadoopConfiguration());
        jobConf.set("dynamodb.servicename", "dynamodb");
        jobConf.set("dynamodb.input.tableName", "ProphecyWithoutIndex");
        jobConf.set("dynamodb.output.tableName", "ProphecyWithoutIndex");
//        jobConf.set("dynamodb.awsAccessKeyId", defaultAWSCredentialsProviderChain.getCredentials().getAWSAccessKeyId());
//        jobConf.set("dynamodb.awsSecretAccessKey", defaultAWSCredentialsProviderChain.getCredentials().getAWSSecretKey());
        jobConf.set("dynamodb.endpoint", "dynamodb.us-east-1.amazonaws.com");
        jobConf.set("dynamodb.regionid", "us-east-1");
        jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat");
        jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat");
        return jobConf;
    }
}
