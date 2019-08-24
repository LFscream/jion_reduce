package com.zzy.jionreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class jionReduce extends Reducer<Text,Text,Text,Text> {

    /**

     *k2      v2
     * p0001  o.text 一行,o.text 一行
     * p0001  p.text一行
     *
     *k3       v3
     *
     * p0001    p.text,o.text.o.text
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        String str1 ="";
        String str2 = "";


        for (Text value : values) {

            String[] split = value.toString().split(",");
            if (split[0].startsWith("P")){


                str1 = value.toString();



            }else{



                str2 += "   "+value.toString();

            }



        }
        context.write(key,new Text(str1+"\t"+str2));















    }
}
