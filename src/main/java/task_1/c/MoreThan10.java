package task_1.c;

import common.Util;
import common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/*
1.c) >10. How many questions are there which have more than 10 words
in their titles?

NOTE TO SELF:
*/


public class MoreThan10 {

    public static void main(String[] args) throws Exception {

        // Check arguments
        if(args.length != 2){
            System.out.println("Wrong number of arguments. Use: <class> <input_path> <output_path>");
            return;
        }

        // Handle log4j exception errors
        org.apache.log4j.BasicConfigurator.configure();

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MoreThan10.class);
        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();

            // Get value of "Body" and "PostTypeId"
            String title = Util.getAttrContent("Title", text);
            String postTypeId = Util.getAttrContent("PostTypeId", text);

            // Check PostTypeId and if body is not an empty string
            if(postTypeId.equals("1") && !title.equals("")){
                // Simple/lazy wordsplit
                String[] words = title.split("\\W+");

                // Write title to context if it has more than 10 words in it.
                if(words.length > 10){
                    context.write(new Text(title), new IntWritable(1));
                }
            }
        }
    }

    private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int totalWords = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            totalWords += sum;
            //context.write(key, new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text summary = new Text("\nAmount of question titles that have 10 or more words in it: ");
            IntWritable sum = new IntWritable(totalWords);
            context.write(summary, sum);
        }
    }

}
