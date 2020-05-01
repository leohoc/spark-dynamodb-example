package com.lcarvalho.sparkddb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.lcarvalho.sparkddb.config.JobConfiguration;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class WriteSampleData {

    private static Logger LOGGER = LogManager.getLogger(WriteSampleData.class);

    public static void main(String[] args) throws Exception {

        String application = "WriteSampleData";
        String tableName = "Prophecy";
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sparkContext = JobConfiguration.buildSparkContext(application, tableName);
        JobConf jobConf = JobConfiguration.build(sparkContext, tableName);

        JavaRDD<String> lines = sparkContext.textFile("s3n://spark-dynamodb-examples/eng_sentences.tsv");
        JavaRDD<String> formattedLines = lines.map(line -> line.split("\t")[2]);
        LOGGER.info("formattedLines count: " + formattedLines.count());

        JavaPairRDD<Text, DynamoDBItemWritable> javaPairRDD = formattedLines.mapToPair(line -> {
            Map<String, AttributeValue> attributes = new HashMap<>();
            attributes.put("prophetCode", new AttributeValue(UUID.randomUUID().toString()));
            attributes.put("prophecyTimestamp", new AttributeValue(LocalDateTime.of(2020,05,02, 0, 0, 0).toString()));
            attributes.put("prophecyDate", new AttributeValue(LocalDate.of(2020,05,02).toString()));
            attributes.put("prophecySummary", new AttributeValue(line));
            attributes.put("prophecyDescription", new AttributeValue(line));

            DynamoDBItemWritable dynamoDBItemWritable = new DynamoDBItemWritable();
            dynamoDBItemWritable.setItem(attributes);
            return new Tuple2<>(new Text(""), dynamoDBItemWritable);
        });

        javaPairRDD.saveAsHadoopDataset(jobConf);
        sparkContext.stop();
    }


}
