import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sun.reflect.generics.tree.Tree;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TFReducer extends Reducer<NullWritable, Text, Text, FloatWritable> {
    private TreeMap<Float, String> tmap2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tmap2 = new TreeMap<Float, String>();
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text val : values){
            String combined = val.toString();
            String[] combineArray = combined.split("/");

            String name = combineArray[0];
            float pct = Float.parseFloat(combineArray[1]);
            tmap2.put(pct, name);

            if (tmap2.size() > 50){
                tmap2.remove(tmap2.firstEntry());
            }
        }

        for (Map.Entry<Float, String> entry: tmap2.entrySet()){
            context.write(new Text(entry.getValue()), new FloatWritable(entry.getKey()));
        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        for (Map.Entry<Float, String> entry: tmap2.entrySet()){
//            float pct = entry.getKey();
//            String name = entry.getValue();
//
//            context.write(new Text(name), new FloatWritable(pct));
//        }
//    }
}
