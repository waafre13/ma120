package task_2.h;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class NamesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        String[] names = (Util.getAttrContent("DisplayName", text)).split("\\s+");

        for (String name : names) {
            context.write(new Text(name), new IntWritable(1));
        }
    }
}
