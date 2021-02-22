import jdk.nashorn.internal.parser.Token;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GDMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private Text outkey = new Text();
    private FloatWritable outval = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] Tokens = input.split(",");

        if(!Tokens[0].equalsIgnoreCase("GAME_ID") && Tokens.length>=20){
//            String position = Tokens[6];
//            String len = String.valueOf(Tokens.length);
//            String[] time = Tokens[8].split(":");
            float fg_pct = Float.parseFloat(Tokens[11]);
//            int minutes = Integer.parseInt(time[0]);
            outkey.set(Tokens[2]);
            outval.set(fg_pct);
            context.write(outkey, outval);

        }


    }
}
