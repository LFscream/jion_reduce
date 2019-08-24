package com.zzy.jjionmap;



import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


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

      private   Map<String,String> map = new HashMap<String,String>();


    //第一件事情:将分布式缓存的小表数据读取到本地Map集合(只需要做一次)


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1.获取分布式缓存中的文件列表,uri列表
        URI[] cacheFiles = context.getCacheFiles();
        //2.获取分布式文件系统

        FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());

        //3.通过分布式文件系统取到文件的输入流
        FSDataInputStream open = fileSystem.open(new Path(cacheFiles[0]));


        //3.1转换成字符串buffer

        BufferedReader br = new BufferedReader(new InputStreamReader(open));

        //4.将文件中的内容写进map集合

        String value = null;
        while ((value=br.readLine())!=null){

            String[] split = value.toString().split(",");
            map.put(split[0],value);


        }
//        关闭字符流
                br.close();






    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //分隔订单文本
        String[] split = value.toString().split(",");

      //从订单文本中获取订单id
        String productId = split[2];

      //获取商品信息
        String productline = map.get(productId);

        //把商品信息和相应的订单信息写回到context中


        context.write(new Text(productId),new Text(productline+"\t"+value));





    }
}
