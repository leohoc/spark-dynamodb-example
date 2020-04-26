# SPARK-DYNAMODB-EXAMPLE

> Development in progress

### Description

Use cases examples of Apache Spark applications ready to run on AWS EMR using data in DynamoDB tables.

#### Install requirements

- An AWS account;
- awscli >= 2 with AWS account credentials configured;
- An installed JDK 8;

#### Creating DynamoDB resources

This command will create a DynamoDB table named 'Prophecy' with a UUID hash key named 'prophetCode', and a Secondary Global Index (SGI) with 'prophecyDate' as the hash key:   

```bash

./dynamodb/create-table.sh

```

#### Creating an EMR cluster

Using the AWS console, create a EMR cluster with the following configurations:

* Cluster execution mode;
* Software configuration version: emr-5.*;
* Applications: Spark;
* Create a EC2 keypair and choose it in the security and access session;
* Add a policy with read/write access to any DynamoDB table and index to the EC2 instance profile Role;

When cluster initializing is complete, add permission to SSH connection:

* Go to the main security group configuration;
* Edit the inbound rules of the master security group;
* Add a rule with 'SSH' type and 'Anywhere' source;

#### Spark App #1: Generating Sample Data

The WriteSampleData application will create more than 1.3 million prophecies in the 'Prophecy' table, evenly spread between two prophecy dates.
Instructions:

1. Create a S3 bucket named 'spark-dynamodb-examples';

2. Upload the 'in/eng_sentences.tsv' file to the bucket;

3. Generate the application jar file:

```bash

./gradlew clean jar

```

4. Upload the generated 'build/libs/WriteSampleData.jar' file to the bucket;

5. Connect to the EMR cluster master node with SSH (click the SSH link in the cluster summary panel and follow the instructions);

6. Download the appliction jar file to the master node:

```bash

aws s3 cp s3://spark-dynamodb-examples/WriteSampleData.jar .

```

7. Execute the application:
 
```bash

spark-submit ./WriteSampleData.jar

```

#### Spark App #2: Counting the number of occurrences of each word in the prophecies of a single day