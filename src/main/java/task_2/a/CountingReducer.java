package task_2.a;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class CountingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int totalUniqueUsers = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        totalUniqueUsers++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text summary = new Text("\nAmount of unique users: ");
        IntWritable sum = new IntWritable(totalUniqueUsers);
        context.write(summary, sum);
    }
}
