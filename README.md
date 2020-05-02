# SPARK-DYNAMODB-EXAMPLE

> Development in progress

### Description

Use cases examples of Apache Spark applications ready to run on AWS EMR using data in DynamoDB tables.

#### Install requirements

- An AWS account;
- awscli >= 2 with AWS account credentials configured;
- An installed JDK 8;

#### Providing the infrastructure

Follow the instructions of the [spark-dynamodb-infrastructure](https://github.com/leohoc/spark-dynamodb-infrastructure) project.

#### Generating the Application Files

##### Spark App #1: Populating the Covid19Citation table

The PopulateCovid19Citations application will store the WHO database of studies with COVID-19 citations in a DynamoDB table.
Instructions:

1. Upload the 'in/WHOCovid19CitationsDatabase.csv' file to the 'spark-dynamodb-example' S3 bucket;

2. Generate the application jar file:

```bash

./gradlew clean fatJarPopulateCitations

```

3. the application file will be generated in build/libs/PopulateCovid19Citations-1.0.jar.

##### Spark App #2: Counting the words in the COVID-19 citations titles 

The Covid19CitationsWordCount application will count the number of times each word was used in the COVID-19 citations titles and print the result in the console.
Instructions:

1. Generate the application jar file:

```bash

./gradlew clean fatJarCitationsWordCount

```

2. the application file will be generated in build/libs/Covid19CitationsWordCount-1.0.jar.

#### Running in the AWS EMR cluster

1. Upload the generated application file to the 'spark-dynamodb-example' bucket;

2. Connect to the EMR cluster master node with SSH (click the SSH link in the cluster summary panel and follow the instructions);

3. Download the application jar file to the master node:

```bash

aws s3 cp s3://spark-dynamodb-example/<app_name>.jar .

```

4. Execute the application:
 
```bash

spark-submit ./<app_name>.jar

```