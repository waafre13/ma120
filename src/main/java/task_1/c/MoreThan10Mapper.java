package task_1.c;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MoreThan10Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        // Get value of "Body" and "PostTypeId"
        String title = Util.getAttrContent("Title", text);
        String postTypeId = Util.getAttrContent("PostTypeId", text);

        // Check PostTypeId and if body is not an empty string
        if (postTypeId.equals("1") && !title.equals("")) {
            // Simple/lazy wordsplit
            String[] words = title.split("\\s+");

            // Write title to context if it has more than 10 words in it.
            if (words.length > 10) {
                context.write(new Text(title), new IntWritable(1));
            }
        }
    }
}
