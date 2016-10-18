package task_1.d;

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
1.d) Stopwords. Based on a), exclude the stopwords from titles (an exam-
ple list can be obtained at: http://j.mp/STOPWORDS). We refer to
the output of this task as to popular words.

NOTE TO SELF:
"PostTypeId" for Q's = 1.
a) was not about titles, it was about body. I'm confused, how is this about a?
*/


public class Stopwords {

    private static String stopwordsPath = "";

    public static void main(String[] args) throws Exception {
        // Handle log4j exception errors
        org.apache.log4j.BasicConfigurator.configure();

        // Check arguments
        if(args.length != 3){
            System.out.println("Wrong number of arguments. Use: <class> <input_path> <output_path> <stopwords_path>");
            return;
        }

        // Initialize stopwords
        stopwordsPath = args[2];

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Stopwords.class);
        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(StopwordsMapper.class);
        job.setReducerClass(StopwordsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    public static String getStopwordsPath(){
        return stopwordsPath;
    }
}
