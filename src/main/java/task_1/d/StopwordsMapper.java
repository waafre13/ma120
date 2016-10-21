package task_1.d;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class StopwordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        // Get value of "Body" and "PostTypeId"
        String body = Util.getAttrContent("Title", text);
        String postTypeId = Util.getAttrContent("PostTypeId", text);

        // Check PostTypeId and if body is not an empty string
        if(postTypeId.equals("1") && !body.equals("")){

            StopwordsReader sr = new StopwordsReader(Stopwords.getStopwordsPath());

            String[] words = body.split("\\s+");

            // Write words to context
            for (String word : words) {

                // Check if word matches any stopword
                if (!sr.getStopwords().contains(word.toLowerCase())){
                    context.write(new Text(word.toLowerCase()), new IntWritable(1));
                }
            }
        }
    }
}
