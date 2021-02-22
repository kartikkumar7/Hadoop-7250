import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MSMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private Text outkey = new Text();
    private FloatWritable outval = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] Tokens = input.split(",");

        if (!Tokens[1].equalsIgnoreCase("GAME_ID") && Tokens.length>=19 && !Tokens[15].isEmpty()){
            outkey.set(Tokens[5]);
            float fgAway = Float.parseFloat(Tokens[15]);
            outval.set(fgAway);
            context.write(outkey, outval);
        }
    }
}
