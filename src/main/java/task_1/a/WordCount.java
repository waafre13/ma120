package task_1.a;

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
1.a) Wordcount. Count the words in the body of questions (note the
PostTypeId). This is the classic Wordcount. The resulting data should
include counts for each word, that is, how many times each word ap-
pears in the body of questions.

NOTE TO SELF:
"PostTypeId" for Q's = 1.
*/


public class WordCount {

    public static void main(String[] args) throws Exception {

        // Check arguments
        if(args.length != 2){
            System.out.println("Wrong number of arguments. Use: <class> <input_path> <output_path>");
            return;
        }

        // Handle log4j exception errors
        org.apache.log4j.BasicConfigurator.configure();

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(WordCount.class);
        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
