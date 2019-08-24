package com.zzy.jionreduce;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * k1       v1
 * 偏移量     text
 *
 * 偏移量     text
 *
 *
 * k2       v2
 *
 * p0001text   小米..text ,20150710
 *
 *
 *
 *
 *
 *
 *
 *
 */
public class jionMapper extends Mapper<LongWritable,Text,Text,Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();


        String name = fileSplit.getPath().getName();
        if (name.equals("product.txt")){

            String[] split = value.toString().split(",");

            String id = split[0];
            context.write(new Text(id),value);


        }else{

            String[] split = value.toString().split(",");

            String id = split[2];
            context.write(new Text(id),value);

        }


    }
}
