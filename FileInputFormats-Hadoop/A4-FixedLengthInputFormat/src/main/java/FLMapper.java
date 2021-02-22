import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FLMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {
    private Text outkey = new Text();
    private Text outval = new Text();

    @Override
    protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

        String outmap = value.toString();
        long outkeyLong = key.get();
        outkey.set(String.valueOf(outkeyLong));
        outval.set(outmap);
        context.write(outkey, outval);
    }
}
