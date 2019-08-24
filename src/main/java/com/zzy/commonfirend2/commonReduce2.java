package com.zzy.commonfirend2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class commonReduce2 extends Reducer<Text, Text, Text, Text> {

    /**
     * k2     v2
     * A-C    <B,D,E>
     * ....
     * A-C
     * <p>
     * <p>
     * <p>
     * <p>
     * <p>
     * <p>
     * <p>
     * k3       v3
     * A-C       B-C-E
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        StringBuffer stringBuffer = new StringBuffer();

        Iterator<Text> iterator = values.iterator();


        for (Text value : values) {


            if (iterator.hasNext()) {
                stringBuffer.append(value.toString()).append("-");

            } else {

                stringBuffer.append(value.toString());

            }


        }
        context.write(key, new Text(stringBuffer.toString()));


    }

}
