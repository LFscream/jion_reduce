package com.zzy.jionreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class jobMain extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new jobMain(), args);
        System.exit(run);
    }


    @Override
    public int run(String[] args) throws Exception {

        //1.启动一个job

        Job jionreduce = Job.getInstance(super.getConf(), "jionreduce");

        jionreduce.setInputFormatClass(TextInputFormat.class);


         //1.
        //设置传入的文件路径和类型

        TextInputFormat.addInputPath(jionreduce,new Path("file:///D:\\input\\jionreduce"));


        //2.设置map
        jionreduce.setMapperClass(jionMapper.class);
        jionreduce.setMapOutputKeyClass(Text.class);
        jionreduce.setMapOutputValueClass(Text.class);

        //3 4 5 6
        //7.设置reduce
        jionreduce.setReducerClass(jionReduce.class);
        jionreduce.setOutputKeyClass(Text.class);
        jionreduce.setOutputValueClass(Text.class);



//        设置输出路径
        TextOutputFormat.setOutputPath(jionreduce,new Path("file:///D:\\out\\jionreduceout10"));

        boolean b = jionreduce.waitForCompletion(true);


        return b?0:1;
    }
}
