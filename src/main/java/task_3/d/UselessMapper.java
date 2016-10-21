package task_3.d;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class UselessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        String postTypeId = Util.getAttrContent("PostTypeId", text);

        // Filter questions
        if(postTypeId.equals("1")){
            String body = Util.getAttrContent("Body", text, true);

            if(body.toLowerCase().contains("useless")){
                context.write(new Text(), new IntWritable(1));
            }
        }
    }
}
