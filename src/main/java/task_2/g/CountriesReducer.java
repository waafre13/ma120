package task_2.g;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class CountriesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int amount = 0;
        for (IntWritable i : values) {
            amount += i.get();
        }

        context.write(new Text(key), new IntWritable(amount));
    }
}
