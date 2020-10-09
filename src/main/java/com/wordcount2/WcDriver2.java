package com.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取一个Job实例
        Job job = Job.getInstance(new Configuration());

        //2.设置类路径(ClassPath)
        job.setJarByClass(WcDriver2.class);

        //3.设置Mapper和Reducer
        job.setMapperClass(WcMapper2.class);
        job.setReducerClass(WcReducer2.class);

        //4.设置Mapper和Reducer的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(WcReducer2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5.设置输入输出数据源
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\IGARASHI\\Desktop\\MapReducer\\wordcount2\\input"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\IGARASHI\\Desktop\\MapReducer\\wordcount2\\output"));

        //6.提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
