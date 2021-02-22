import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SSReducer extends Reducer<PlayerMinutePair, FloatWritable, Text, Text> {
    private Text outkey = new Text();
    private Text outval = new Text();

    @Override
    protected void reduce(PlayerMinutePair key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        for (FloatWritable val: values){
            String name = key.playerName;
            float minutes = key.minutes;
            float points = val.get();
            String ok = name + " | " + String.valueOf(minutes);
            outkey.set(ok);
            outval.set(String.valueOf(points));
            context.write(outkey, outval);
        }


    }
}
