package com.app;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

public class ParquetReaderEngine {


    /**
     * Printing parquet contents
     *
     * @param parquetFilePath path to parquet file for reading
     * @param limitToShow     number of maximum rows will be displayed
     */
    public static void readParquetFile(String parquetFilePath, int limitToShow) {
        Path path = new Path(parquetFilePath);
        ParquetReader<GenericData.Record> reader = null;

        try {
            reader = AvroParquetReader
                    .<GenericData.Record>builder(path)
                    .withConf(new Configuration())
                    .build();

            GenericData.Record record;
            for (; limitToShow > 0 && (record = reader.read()) != null; limitToShow++) {
                System.out.println(record);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
