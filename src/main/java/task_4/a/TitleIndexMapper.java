package task_4.a;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class TitleIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        String[] words = Util.getAttrContent("Title", text).split("\\W+");
        String rowId = Util.getAttrContent("Id", text);
        int id = Util.isInteger(rowId) ? Integer.parseInt(rowId) : -1;

        for (String word : words) {
            context.write(new Text(word.toLowerCase()), new IntWritable(id));
        }
    }
}
