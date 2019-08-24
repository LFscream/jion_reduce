package com.zzy.commonfirend2;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


/**
 *
 *
 *
 *
 *
 *
 *
 *
 *k2       v2
 * A-C      B
 * A-E      B
 *
 *
 *
 *
 *
 *
 *
 *
 */
public class commonMapper2 extends Mapper<LongWritable,Text,Text,Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        String s = split[1];


//       split1 [A,F,C,J,E]
        String[] split1 = split[0].split("-");

        Arrays.sort(split1);
        // [A,C,E,F,J]

        //A-C
          //A-E
            //A-F
             //A-J
               //C-E
                 //C-F..

        for (int i=0;i<split1.length-1;i++){
            for (int j=i+1;j<split1.length;j++){

                context.write(new Text(split1[i]+"-"+split1[j]),new Text(s));



            }
        }




        //split[1]作为v2









  

    }
}
