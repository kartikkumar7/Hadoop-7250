import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class nyseReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    FloatWritable result = new FloatWritable();

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float maxValue = 0;
        for (FloatWritable val : values){
            if(maxValue < val.get()){
                maxValue = val.get();
            }
        }
        result.set(maxValue);
        context.write(key, result);
    }
}
