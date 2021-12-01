#### Please watch Topic 1 and 2 of Lesson 8 of pro-Spark Course.
#### pro-Spark-aws Code Structure.
#### create Data Pipeline and populate KPI Tables in Sink.

#### packages
- cloud : has cloud connectors
- common :
- core :
  Application Main - Entry Point
  ProSpark App - Spark Transformations
- utils : Utility packages such as LogFormatter, Resources

#### Config file
application.yml - This is the Configuration which has all the configurations required by the Application such as cloud type, projectId, type of persistent storage, input location, schema location, kpi location

#### ddl file
- schema.ddl defines the schema for your dataset.
- If you bring your own dataset, define the schema related in this file.

#### Classes and Objects in Project
**ApplicationMain** is the main file and entry point to pro-Spark-aws application.

**CloudInitializer** - has initializeCloud().
- It reads the application.yml and populates cloudConfig case class object.
- AWSConnector returns cloudConnector object by taking cloudConfig as parameter.

**ProSparkApp** 
- Reads the schema from cloud object storage
- Create Spark Session
- Reads the data from input location into Spark Objects
- Performs Data, Schema and Business Validations
- Applies Spark Transformations on the validated data and stores the transformed data into Amazon S3, which is queried by Amazon Athena extenal KPI Tables.

**Insight**
- Reads the SQl query defined in kpiquery.json and executes using Spark SQL.
- The results are captured in a dataframe and then stored in Amazon S3.

#### Creation of Data Pipeline
**Data Integration**

SanKir SDF converter reads the raw csv data in Amazon S3. It then converts it to json format with additional metadata and stores it in processed-retail-data folder in Project directory of Amazon S3.

**Data Transformation**

ProSparkApp reads the processed json data, performs Validations, Applies Spark Transformations based on KPI defined in kpiquery.json.

**Sink**

- ProSparkApp then stores the transformed data in sink.
- Here Amazon S3 acts as sink