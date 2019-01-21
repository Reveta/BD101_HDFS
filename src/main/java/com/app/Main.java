package com.app;

import org.apache.avro.Schema;

public class Main {

    public static void main(String[] args) {

        //parsing avro schema
        Schema schema = ParquetUtils.parseSchema(args[0]);

        //converting csv(args[1]) to parquet file(args[2]) using avro schema(args[0])
        ParquetUtils.writeToParquet(schema,args[1],args[2]);

        //printing content of generated parquet file(args[2]) with some maximum rows(args[3])
        ParquetUtils.readParquetFile(args[2],Integer.parseInt(args[3]));

    }

}