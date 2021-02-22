import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GDMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outkey = new Text();
    private Text outval = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] Tokens = input.split(",");

        String gameId = Tokens[0];
        String playerName = Tokens[5];

        if (!gameId.isEmpty() && !playerName.isEmpty() && Tokens.length>=27 && !Tokens[0].equalsIgnoreCase("GAME_ID")){
            float pt3 = Float.parseFloat(Tokens[12]);
            float pt2 = Float.parseFloat(Tokens[9]) - Float.parseFloat(Tokens[12]);
            float pt1 = Float.parseFloat(Tokens[15]);

            float totalPts = pt1*1 + pt2*2 + pt3*3;
            if (totalPts >= 40){
                outkey.set(gameId);
                outval.set("G"+playerName);
                context.write(outkey, outval);
            }

        }
    }
}
