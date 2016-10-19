package task_2.i;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class AnswersReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int questions = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable i : values) {
            questions += i.get();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Generate string output
        String summary = ("Amount of questions that have at least 1 (one) answer:");

        context.write(new Text(summary), new IntWritable(questions));
    }
}
