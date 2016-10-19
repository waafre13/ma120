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

        int score = Integer.parseInt(Util.getAttrContent("Score", text));
        int postTypeId = Integer.parseInt(Util.getAttrContent("PostTypeId", text));
        String title = Util.getAttrContent("Title", text);

        if(postTypeId == 1){
            context.write(new Text(title), new IntWritable(score));
        }
    }
}
