package task_4.a;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

class TitleIndexReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String totalQ = "";
        for (IntWritable i : values) {
            totalQ += totalQ.equals("") ? i : ","+i;
        }

        context.write(new Text(key), new Text(totalQ));
    }
}
