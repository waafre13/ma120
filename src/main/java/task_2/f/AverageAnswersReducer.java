package task_2.f;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class AverageAnswersReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private double totalA = 0;
    private double totalQ = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable i : values) {
            totalA += i.get();
        }
        totalQ++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Generate string output
        String summary = ("Average amount of answers per question is:");
        double sum = totalA/totalQ;

        context.write(new Text(summary), new DoubleWritable(sum));
    }
}
