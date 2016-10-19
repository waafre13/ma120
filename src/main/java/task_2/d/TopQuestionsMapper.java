package task_2.d;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class TopQuestionsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        int postTypeId = Integer.parseInt(Util.getAttrContent("PostTypeId", text));

        if(postTypeId == 1){
            String title = Util.getAttrContent("Title", text);

            //TODO: Check if string is integer first?
            int score = Integer.parseInt(Util.getAttrContent("Score", text));
            context.write(new Text(title), new IntWritable(score));
        }
    }
}
