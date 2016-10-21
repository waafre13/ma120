package task_2.a;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class CountingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String id = Util.getAttrContent("AccountId", value.toString());
        if (Util.isInteger(id)){
            context.write(new Text(id), new IntWritable(1));
        }
    }
}
