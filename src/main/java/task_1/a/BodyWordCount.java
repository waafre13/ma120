package task_1.a;

import common.XmlInputFormat;
import common.Util;
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


public class BodyWordCount {

    public static void main(String[] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(BodyWordCount.class);
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
            String body = Util.getAttrContent("Body", text);
            String postTypeId = Util.getAttrContent("PostTypeId", text);

            // Check PostTypeId and if body is not an empty string
            if(postTypeId.equals("1") && !body.equals("")){
                String[] words = body.split("\\W+");
                int totalWords = words.length;

                // Writes the body's content and its word count.
                context.write(new Text(body), new IntWritable(totalWords));
            }
        }
    }

    private static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {

        private int totalWords = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            totalWords += sum;
            //context.write(key, new LongWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text summary = new Text("Total words: ");
            LongWritable sum = new LongWritable((long)totalWords);
            context.write(summary, sum);
        }
    }

}
