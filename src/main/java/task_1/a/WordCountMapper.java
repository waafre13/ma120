package task_1.a;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        // Get value of "Body" and "PostTypeId"
        String body = Util.getAttrContent("Body", text, true);
        String postTypeId = Util.getAttrContent("PostTypeId", text);

        // Check PostTypeId and if body is not an empty string
        if (postTypeId.equals("1") && !body.equals("")) {
            // Simple/lazy wordsplit
            String[] words = body.split("\\s+");

            // Write words to context
            for (String word : words) {
                context.write(new Text(word.toLowerCase()), new IntWritable(1));
            }
        }
    }
}
