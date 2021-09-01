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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;

import javax.xml.crypto.Data;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.spark.sql.functions.*;

public class PaywareGLAFileSummarizer {

    private static final String APP_NAME = "PaywareGLAFileSummarizer";

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        // Building the Spark session
        JavaSparkContext javaSparkContext = SparkConfiguration.buildLocalSparkContext(APP_NAME);
        SparkSession sparkSession = SparkSession.builder().sparkContext(javaSparkContext.sc()).getOrCreate();
        DataFrameReader dataFrameReader = sparkSession.read();

        Dataset<Row> fullCitations = dataFrameReader.option("header","true").csv("in/GLA_00000000001_20210830.DAT");
        fullCitations.show(10);

        // Loading the CSV citations file in a Dataset
        JavaRDD<String> rawRows = javaSparkContext.textFile("in/GLA_00000000001_20210830.DAT");

        JavaRDD<AccountingEntry> accountingEntriesRDD = rawRows
                .filter(line -> !line.startsWith("FHA000000"))
                .filter(line -> line.length() > 130)
                .map(line -> new AccountingEntry(
                        line.substring(37, 67).trim(),
                        line.substring(68, 98).trim(),
                        BigDecimal.valueOf(Long.parseLong(line.substring(116, 130).trim()))));

        Dataset<AccountingEntry> accountingEntries = sparkSession.createDataset(accountingEntriesRDD.rdd(), Encoders.bean(AccountingEntry.class));
        accountingEntries.show(10);

        Dataset<AccountingEntry> filteredAccountingEntries = accountingEntries.filter(col("debitAccount").equalTo("21611017"));
        filteredAccountingEntries.show(10);

        RelationalGroupedDataset groupedAccountingEntries = filteredAccountingEntries.groupBy(col("debitAccount"), col("creditAccount"));
        groupedAccountingEntries.sum("value").show();

        sparkSession.stop();
    }
}