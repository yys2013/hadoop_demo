package com.hadp.wc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountSort {

    public static class TokenizerMapper extends
            Mapper<Object, Text, WordCountBean, NullWritable> {

        private WordCountBean wordCountBean = new WordCountBean();

        public void map(
                Object key,
                Text value,
                Mapper<Object, Text, WordCountBean, NullWritable>.Context context)
                throws IOException, InterruptedException {
            String[] fields = StringUtils.split(value.toString(), "\t");
            System.out.println("===============fields1: " + fields.length);
            if(fields != null && fields.length == 2) {
                wordCountBean.setWord(fields[0]);
                wordCountBean.setWordCount(Integer.parseInt(fields[1]));
                context.write(wordCountBean, NullWritable.get());
            }
        }
    }

    public static class IntSumReducer extends
            Reducer<WordCountBean, NullWritable, WordCountBean, NullWritable> {
        public void reduce(WordCountBean key, Iterable<NullWritable> values,
                Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count sort");
        job.setJarByClass(WordCountSort.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(WordCountBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://10.10.22.176:9000/user/aifjs/data/output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://10.10.22.176:9000/user/aifjs/data/output2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class WordCountBean implements WritableComparable<WordCountBean> {

    private String word;
    private int wordCount;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getWordCount() {
        return wordCount;
    }

    public void setWordCount(int wordCount) {
        this.wordCount = wordCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(wordCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        wordCount = in.readInt();
    }

    @Override
    public int compareTo(WordCountBean o) {
        if (wordCount - o.getWordCount() == 0) {
            return word.compareTo(o.getWord());
        } else {
            return wordCount - o.getWordCount();
        }
    }

    @Override
    public String toString() {
        return word + "\t" + wordCount;
    }

}