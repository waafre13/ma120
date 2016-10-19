package task_1.b;

import common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
1.b) Unique words. Write a Hadoop MapReduce job that outputs words
in the question titles. The output should contain all words used in the
title of questions, only once. No count, just the word. That will the
dictionary over titles of the questions.

NOTE TO SELF:

*/


public class UniqueWords {

    public static void main(String[] args) throws Exception {

        // Check arguments
        if(args.length != 2){
            System.out.println("Wrong number of arguments. Use: <class> <input_path> <output_path>");
            return;
        }

        // Handle log4j exception errors
        org.apache.log4j.BasicConfigurator.configure();

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(UniqueWords.class);
        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(UniqueWordsMapper.class);
        job.setReducerClass(UniqueWordsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
