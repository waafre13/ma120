package task_3.a;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class BigramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        String postTypeId = Util.getAttrContent("PostTypeId", text);

        // Filter questions
        if(postTypeId.equals("1")){
            String[] words = Util.getAttrContent("Title", text).split("\\s+");

            String prevWord = "";
            for (String word: words) {
                if(!prevWord.equals("")){
                    String bigram = prevWord+" "+word;
                    context.write(new Text(bigram.toLowerCase()), new IntWritable(1));
                }
                prevWord = word;
            }
        }
    }
}
