package com.lcarvalho.sparkddb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.lcarvalho.sparkddb.config.SparkConfiguration;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.spark.sql.functions.col;

public class PopulateCovid19Citations {

    private static final String APP_NAME = "PopulateCovid19Citations";
    private static Logger LOGGER = LogManager.getLogger(PopulateCovid19Citations.class);

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        // Building the Spark session
        JavaSparkContext javaSparkContext = SparkConfiguration.buildSparkContext(APP_NAME);
        SparkSession sparkSession = SparkSession.builder().sparkContext(javaSparkContext.sc()).getOrCreate();
        DataFrameReader dataFrameReader = sparkSession.read();

        // Loading the CSV citations file in a Dataset
        Dataset<Row> fullCitations = dataFrameReader.option("header","true").csv("s3n://spark-dynamodb-example/WHOCovid19CitationsDatabase.csv");
        fullCitations.show(10);

        // Selecting only the relevant columns for our exercise
        Dataset<Row> citations = fullCitations.select(
                                        col("Title"),
                                        col("Authors"),
                                        col("Abstract"),
                                        col("Published Year"),
                                        col("Journal"),
                                        col("Study"),
                                        col("Tags"));
        citations.show(10);
        LOGGER.info("Citations count: " + citations.count());

        // Removing citations with null title
        Dataset<Row> filteredCitations = citations.filter(col("Title").isNotNull());
        LOGGER.info("Filtered citations count: " + filteredCitations.count());

        // Building a RDD composed of DynamoDB writable items that matches the Covid19Citation table
        JavaPairRDD<Text, DynamoDBItemWritable> dynamoCitations = filteredCitations.javaRDD().mapToPair(citation -> {
            Map<String, AttributeValue> attributes = new HashMap<>();
            putStringAttribute(attributes, "citationCode", UUID.randomUUID().toString());
            putStringAttribute(attributes, "title", citation.getAs("Title"));
            putStringAttribute(attributes, "authors", citation.getAs("Authors"));
            putStringAttribute(attributes, "abstract", citation.getAs("Abstract"));
            putNumberAttribute(attributes, "publishedYear", citation.getAs("Published Year"));
            putStringAttribute(attributes, "journal", citation.getAs("Journal"));
            putStringAttribute(attributes, "study", citation.getAs("Study"));
            putStringAttribute(attributes, "tags", citation.getAs("Tags"));

            DynamoDBItemWritable dynamoDBItemWritable = new DynamoDBItemWritable();
            dynamoDBItemWritable.setItem(attributes);
            return new Tuple2<>(new Text(""), dynamoDBItemWritable);
        });

        // Writing data to the DynamoDB table
        JobConf jobConf = SparkConfiguration.buildJobConf(javaSparkContext, Boolean.FALSE);
        dynamoCitations.saveAsHadoopDataset(jobConf);
        sparkSession.stop();
    }

    private static void putStringAttribute(Map<String, AttributeValue> attributes, String key, Object fieldValue) {
        if (fieldValue != null) {
            attributes.put(key, new AttributeValue(fieldValue.toString()));
        }
    }

    private static void putNumberAttribute(Map<String, AttributeValue> attributes, String key, Object fieldValue) {
        if (fieldValue != null) {
            try {
                Integer integerFieldValue = Integer.parseInt(fieldValue.toString());
                AttributeValue attributeValue = new AttributeValue();
                attributeValue.setN(integerFieldValue.toString());
                attributes.put(key, attributeValue);
            } catch (Exception e) {
                LOGGER.info("cannot convert " + fieldValue + " to integer");
            }
        }
    }
}