import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class kvReducer extends Reducer<Text, Text, Text, IntWritable> {
    IntWritable sum = new IntWritable(0);
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Text val : values){
            count++;
        }
        sum.set(count);
        context.write(key,sum);
    }
}
