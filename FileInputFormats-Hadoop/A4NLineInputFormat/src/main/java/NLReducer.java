import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NLReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable sum = new IntWritable(0);
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int add = 0;
        for (IntWritable val : values){
            add += val.get();
        }
        sum.set(add);
        context.write(key,sum);
    }
}
