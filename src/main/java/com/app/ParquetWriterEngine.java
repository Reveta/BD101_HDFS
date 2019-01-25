package com.app;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetWriterEngine {

    /**
     * Convert csv to parquet and write to file
     *
     * @param schemaPath  need to create schema for new parquet file
     * @param csvPath     the path to csv is going to be coded
     * @param parquetPath the path to new parquet file with csv data
     */
    public static void writeToParquet(String schemaPath, String csvPath, String parquetPath) {
        /* create schema for new parquet file */
        Schema schema = parseSchema(schemaPath);

        /**/
        Path path = new Path(parquetPath);
        org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = null;
        GenericData.Record record;

        try (
                Reader reader = Files.newBufferedReader(Paths.get(csvPath));
                CSVReader csvReader = new CSVReaderBuilder(reader)
                        .withSkipLines(1)
                        .build()
        ) {

            //Building writer
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build();


            String[] nextRecord;
            while ((nextRecord = csvReader.readNext()) != null) {
                record = new GenericData.Record(schema);

                for (int i = 0; i < nextRecord.length; i++) {
                    Schema.Type schemaType = schema.getFields().get(i).schema().getType();
                    boolean isNumber = NumberUtils.isNumber(nextRecord[i]);

                    //Detecting data type and putting appropriate record
                    record = putDataToRecord(record, i, schemaType, isNumber, nextRecord);
                }
                writer.write(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Returns org.apache.avro.Schema by schema.avsc
     *
     * @param schemaPath path to avsc file
     *
     * @return schema
     */
    static Schema parseSchema(String schemaPath) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;

        try {
            schema = parser.parse(new File(schemaPath));

        } catch (IOException e) {
            e.printStackTrace();
        }

        return schema;
    }

    /**
     *Put all type of data in record and get back it
     *
     * @param record - put data in this record
     * @param key - key
     * @param schemaType - type of data
     *
     * @return record - with new data
     */
    private static GenericData.Record putDataToRecord(GenericData.Record record, int key, Schema.Type schemaType, boolean isNumber, String[] nextRecord) {
        switch (schemaType) {
            case INT:
                int putRecordInt = isNumber ? Integer.parseInt(nextRecord[key]) : 0;
                record.put(key, putRecordInt);
                break;
            case DOUBLE:
                double putRecordDouble = isNumber ? Double.parseDouble(nextRecord[key]) : 0.0d;
                record.put(key, putRecordDouble);
                break;
            case LONG:
                long putRecordLong = isNumber ? Long.parseLong(nextRecord[key]) : 0L;
                record.put(key, putRecordLong);
                break;
            case STRING:
                record.put(key, nextRecord[key]);
                break;
        }
        return record;
    }

}