import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SSMapper extends Mapper<LongWritable, Text, PlayerMinutePair, FloatWritable> {
    private PlayerMinutePair outkey = new PlayerMinutePair();
    private FloatWritable outval = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] Tokens = input.split(",");

        if (!Tokens[0].equalsIgnoreCase("GAME_ID") && Tokens.length>=20){
            String name = Tokens[5];
            float pt3 = Float.parseFloat(Tokens[12]);
            float pt2 = Float.parseFloat(Tokens[9]) - Float.parseFloat(Tokens[12]);
            float pt1 = Float.parseFloat(Tokens[15]);

            float ptTotal = pt1*1 + pt2*2 + pt3*3;

            String[] time = Tokens[8].split(":");
            float minutes = Float.parseFloat(time[0]);

            outkey.set(name,minutes);
            outval.set(ptTotal);

            context.write(outkey, outval);
        }
    }
}
