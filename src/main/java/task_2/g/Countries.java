package task_2.g;

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
2.g) Countries. Discover users by countries, that is the output should be
country and the number of users.

Run command:

*/

public class Countries {

    public static void main(String[] args) throws Exception {

        // Check arguments
        if(args.length != 2){
            System.out.println("Wrong number of arguments. Use: <class> <input_path> <output_path>");
            return;
        }

        // Handle log4j exception errors
        org.apache.log4j.BasicConfigurator.configure();

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Countries.class);
        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(CountriesMapper.class);
        job.setReducerClass(CountriesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
