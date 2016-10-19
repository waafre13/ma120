package task_3.d;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class UselessReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int totalQ = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable i : values) {
            totalQ += i.get();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        context.write(new Text("Total amount of questions that contains the word 'useless':"), new IntWritable(totalQ));
    }
}
