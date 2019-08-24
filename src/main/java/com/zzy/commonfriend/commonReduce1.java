package com.zzy.commonfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class commonReduce1 extends Reducer<Text,Text,Text,Text> {

    /**

     *k2     v2
     *
     * O      <i,j>
     *
     *
     *
     *
     *
     *
     *k3       v3
     *<i-j- >    O
     *
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





        for (Text value : values) {

            stringBuffer.append(value.toString()).append("-");


        }
















    context.write(new Text(stringBuffer.toString()),key);












    }
}
