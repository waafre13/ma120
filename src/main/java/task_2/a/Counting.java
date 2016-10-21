package task_2.a;

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
2.b) Unique users. Write a Hadoop MapReduce job that outputs unique
users in the dataset.

-----
hadoop jar ma120/hadoop-xml-reader/target/hadoop-custom_recordreader-1.0-SNAPSHOT.jar task_2.a.UniqueUsers stackexchange/Users.xml task_2b
=====
Amount of unique users: 	83317
-----

*/


public class UniqueUsers {

    public static void main(String[] args) throws Exception {

        // Check arguments
        if(args.length != 2){
            System.out.println("Wrong number of arguments. Use: <class> <input_path> <output_path>");
            return;
        }

        // Handle log4j exception errors
        org.apache.log4j.BasicConfigurator.configure();

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(UniqueUsers.class);
        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(UniqueUsersMapper.class);
        job.setReducerClass(UniqueUsersReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
