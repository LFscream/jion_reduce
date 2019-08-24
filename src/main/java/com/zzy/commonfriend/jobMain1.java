package com.zzy.commonfriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class jobMain1 extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new jobMain1(), args);
        System.exit(run);
    }


    @Override
    public int run(String[] args) throws Exception {

        //1.启动一个job

        Job commonfriend1 = Job.getInstance(super.getConf(), "commonfriend1");

        commonfriend1.setInputFormatClass(TextInputFormat.class);


         //1.
        //设置传入的文件路径和类型

        TextInputFormat.addInputPath(commonfriend1,new Path("file:///D:\\input\\findcommon"));


        //2.设置map
        commonfriend1.setMapperClass(commonMapper1.class);
        commonfriend1.setMapOutputKeyClass(Text.class);
        commonfriend1.setMapOutputValueClass(Text.class);

        //3 4 5 6
        //7.设置reduce
        commonfriend1.setReducerClass(commonReduce1.class);
        commonfriend1.setOutputKeyClass(Text.class);
        commonfriend1.setOutputValueClass(Text.class);



//        设置输出路径
        TextOutputFormat.setOutputPath(commonfriend1,new Path("file:///D:\\out\\findcommon1_out2"));

        boolean b = commonfriend1.waitForCompletion(true);


        return b?0:1;
    }
}
