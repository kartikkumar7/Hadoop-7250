import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class kvMapper extends Mapper<Text, Text, Text, Text> {
//    IntWritable result = new IntWritable();
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//        int val = Integer.parseInt(value.toString());
//        result.set(val);
        context.write(key,value);
    }
}
