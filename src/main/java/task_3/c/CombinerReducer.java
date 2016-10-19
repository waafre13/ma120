package task_3.c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int totalWords = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        totalWords += sum;
        context.write(key, new IntWritable(sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text summary = new Text("\nTotal amount of words: ");
        IntWritable sum = new IntWritable(totalWords);
        context.write(summary, sum);
    }
}
