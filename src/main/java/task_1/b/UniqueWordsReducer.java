package task_1.b;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class UniqueWordsReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // Output only the key
        context.write(key, NullWritable.get());
    }
}
