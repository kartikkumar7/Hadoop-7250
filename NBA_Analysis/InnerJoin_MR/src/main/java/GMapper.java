import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class GMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outkey = new Text();
    private Text outval = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] Tokens = input.split(",");

        String gameId = Tokens[1];
        String season = Tokens[5];

        if (!gameId.isEmpty() && !season.isEmpty()){
            outkey.set(gameId);
            outval.set("S"+season);
            context.write(outkey, outval);
        }
    }
}
