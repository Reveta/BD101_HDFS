# BD101_Spark_core_1
Task1: Using Java / Scala / Python convert data from lesson 2 into Avro or Parquet and save as a copy


Java program.
For build use maven command:
mvn clean compile assembly:single
package

For start:

 1) without arguments - program will be started with test data:
        pathToTrainSchema = "src/main/resources/train.avsc";
        pathToTestTrain = "src/main/resources/trainSmall.csv";
        pathToNewParquetFile = "parquetFiles/testParquet_" + new Date().getTime() + ".parquet";
        limitToShowDefault = 5;

 2) with arguments:
        agrg(0) - path to avro Schema of csv file(you can find examples at resources folder)
        agrg(1) - path to csv what you want write to parquet
        agrg(2) - path to your new parquet file(not exist before program start)
        agrg(3) - limit, how much row, of data from new parquet file, show to console