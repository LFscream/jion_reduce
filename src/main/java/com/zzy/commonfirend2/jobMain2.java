package com.zzy.commonfirend2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class jobMain2 extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new jobMain2(), args);
        System.exit(run);
    }


    @Override
    public int run(String[] args) throws Exception {

        //1.启动一个job

        Job commonfriend2 = Job.getInstance(super.getConf(), "commonfriend2");

        commonfriend2.setInputFormatClass(TextInputFormat.class);


         //1.
        //设置传入的文件路径和类型

        TextInputFormat.addInputPath(commonfriend2,new Path("file:///D:\\out\\findcommon1_out2"));


        //2.设置map
        commonfriend2.setMapperClass(commonMapper2.class);
        commonfriend2.setMapOutputKeyClass(Text.class);
        commonfriend2.setMapOutputValueClass(Text.class);

        //3 4 5 6
        //7.设置reduce
        commonfriend2.setReducerClass(commonReduce2.class);
        commonfriend2.setOutputKeyClass(Text.class);
        commonfriend2.setOutputValueClass(Text.class);



//        设置输出路径
        TextOutputFormat.setOutputPath(commonfriend2,new Path("file:///D:\\out\\findcommon2_out5"));

        boolean b = commonfriend2.waitForCompletion(true);


        return b?0:1;
    }
}
