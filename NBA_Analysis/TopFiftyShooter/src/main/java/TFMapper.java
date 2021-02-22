import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.reflect.generics.tree.Tree;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TFMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private TreeMap<Float, String> tmap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tmap = new TreeMap<Float, String>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] Tokens = input.split(",");

        if (!Tokens[0].equalsIgnoreCase("GAME_ID") && Tokens.length>=20 && !Tokens[12].isEmpty()){
            String name = Tokens[5];

            float pt3 = Float.parseFloat(Tokens[12]);
            float pt2 = Float.parseFloat(Tokens[9]) - Float.parseFloat(Tokens[12]);
            float pt1 = Float.parseFloat(Tokens[15]);

            float shootPct = pt1*1 + pt2*2 + pt3*3;
//            float shootPct = Float.parseFloat(Tokens[11]);

            tmap.put(shootPct, name);

            if (tmap.size() > 50){
                tmap.remove(tmap.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Float, String> entry: tmap.entrySet()){
            float pct = entry.getKey();
            String name = entry.getValue();
            String outval = name + "/" + (String.valueOf(pct));
            context.write(NullWritable.get(), new Text(outval));
        }
    }
}
