package task_3.b;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class TrigramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        String postTypeId = Util.getAttrContent("PostTypeId", text);

        // Filter questions
        if(postTypeId.equals("1")){
            String[] words = Util.getAttrContent("Title", text).split("\\s+");

            String prevWord = "";
            String prevPrevWord = "";
            for (String word: words) {
                if(!prevWord.equals("") && !prevPrevWord.equals("")){
                    String trigram = prevPrevWord+" "+prevWord+" "+word;
                    context.write(new Text(trigram.toLowerCase()), new IntWritable(1));
                }
                prevPrevWord = prevWord;
                prevWord = word;
            }
        }
    }
}
