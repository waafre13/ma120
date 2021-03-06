package task_1.f;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import task_1.d.Stopwords;
import task_1.d.StopwordsReader;

import java.io.IOException;

class TagsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        String tags = Util.getAttrContent("Tags", text);

        // Check if tags is not an empty string
        if(!tags.equals("")){
            String[] words = tags.split("\\s+");

            // Write words to context
            for (String word : words) {
                context.write(new Text(word.toLowerCase()), new IntWritable(1));
            }
        }
    }
}
