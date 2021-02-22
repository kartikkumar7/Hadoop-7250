import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outkey = new Text();
    private Text outval = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] Tokens = value.toString().split(",");

        if (!Tokens[0].equalsIgnoreCase("exchange")) {
            outkey.set(Tokens[1]);
            float curr_volume = Float.valueOf(Tokens[7]);
            float curr_adj = Float.valueOf(Tokens[8]);
            String outstring = curr_volume + "," + curr_volume + "," + curr_adj;
            outval.set(outstring);
            context.write(outkey, outval);
        }

    }
}
