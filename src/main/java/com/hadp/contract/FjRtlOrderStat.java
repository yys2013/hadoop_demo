package com.hadp.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FjRtlOrderStat {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args)
//                .getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: wordcount <in> [<in>...] <out>");
//            System.exit(2);
//        }
        Job job = Job.getInstance(conf, "FjRtlOrderStat");
        job.setJarByClass(FjRtlOrderStat.class);
        job.setMapperClass(FjRtlOrderStatMapper.class);
        job.setCombinerClass(FjRtlOrderStatCombiner.class);
        job.setReducerClass(FjRtlOrderStatReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FjRtlOrderBean.class);
        job.setOutputKeyClass(FjRtlOrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        
        conf.set("fs.defaultFS", "hdfs://10.10.22.176:9000");
        FileInputFormat.addInputPath(job, new Path("hdfs://10.10.22.176:9000/user/aifjs/fjsdb/fj_rtl_order/part-m-00000"));
        Path path = new Path("hdfs://10.10.22.176:9000/user/aifjs/data/fj_rtl_order_stat_out");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(path)) {
            fs.delete(path, true);
        }
        fs.close();
        FileOutputFormat.setOutputPath(job, path);
        
        
//        for (int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//        }
//        FileOutputFormat.setOutputPath(job, new Path(
//                otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}