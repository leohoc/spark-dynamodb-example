package com.lcarvalho.sparkddb;

import com.lcarvalho.sparkddb.config.JobConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

public class WordCountAudience {

    private static Logger LOGGER = LogManager.getLogger(WordCountAudience.class);

    public static void main(String[] args) throws Exception {

        String application = "WordCountAudience";
        String tableName = "Prophecy";
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sparkContext = JobConfiguration.buildSparkAudienceContext(application, tableName);
        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();

        Dataset prophecies = sparkSession.read().option("tableName", tableName).format("dynamodb").load();
        Dataset filteredProphecies = prophecies.filter(col("prophecyDate").equalTo("2020-05-02"));

        LOGGER.info("filtered prophecies count: " + filteredProphecies.count());

        Dataset propheciesSummaries = filteredProphecies.select(col("prophecySummary"));

//        propheciesSummaries.show(10);

        JavaRDD<String> words = propheciesSummaries.javaRDD().flatMap(prophecySummary -> Arrays.asList(prophecySummary.toString().split(" ")).iterator());
        LOGGER.info("prophecies summaries word count: " + words.count());

        Map<String, Long> wordCounts = words.countByValue();
        LOGGER.info("prophecies summaries distinct word count: " + wordCounts.size());
    }
}
