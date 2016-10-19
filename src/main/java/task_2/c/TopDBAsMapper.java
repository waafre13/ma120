package task_2.c;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

class TopDBAsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        //TODO: Check if string is integer first?
        int rep = Integer.parseInt(Util.getAttrContent("Reputation", text));
        String name = Util.getAttrContent("DisplayName", text);

        context.write(new Text(name), new IntWritable(rep));
    }
}
