package com.app;

import java.util.Date;

public class Main {

    private static final String pathToTrainSchema = "src/main/resources/train.avsc";
    private static final String pathToTestTrain = "src/main/resources/trainSmall.csv";
    private static final String pathToNewParquetFile = "parquetFiles/testParquet_" + new Date().getTime() + ".parquet";
    private static final int limitToShowDefault = 5;

    public static void main(String[] args) {

        boolean argsExist = args.length != 0;
        String schemaPath = argsExist ? args[0] : pathToTrainSchema;
        String csvFilePath = argsExist ? args[1] : pathToTestTrain;
        String newParquetFilePath = argsExist ? args[2] : pathToNewParquetFile;
        int limitToShow = argsExist ? Integer.valueOf(args[3]) : limitToShowDefault;


        /** converting csv file{csvFilePath}
         *  to new parquet file{newparquetFilePath}
         *  using avro schema{schemaPath} */
        ParquetWriterEngine.writeToParquet(schemaPath, csvFilePath, newParquetFilePath);

        /** show data from parquet file{newParquetFilePath}
         * {limitToShow} is number of rows you want show
         * */
        ParquetReaderEngine.readParquetFile(newParquetFilePath, limitToShow);

    }

}