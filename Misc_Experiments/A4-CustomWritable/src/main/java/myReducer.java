import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myReducer extends Reducer<Text, Text, Text, Text> {
    private Text outval = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        float min_stock_volume = Float.MAX_VALUE;
        float max_stock_volume = 0;
        float max_adj_close = 0;
        for (Text t :values) {
            String[] vals = t.toString().split(",");

            float curr_volume_1 = Float.valueOf(vals[0]);
            float curr_volume_2 = Float.valueOf(vals[1]);
            float curr_adj_close = Float.valueOf(vals[2]);

            if (curr_volume_1 > max_stock_volume) {
                max_stock_volume = curr_volume_1;
            }
            if (curr_volume_2 < min_stock_volume) {
                min_stock_volume = curr_volume_2;
            }
            if (curr_adj_close > max_adj_close) {
                max_adj_close = curr_adj_close;
            }
        }
        String max_vol = String.valueOf(max_stock_volume);
        String min_vol = String.valueOf(min_stock_volume);
        String max_adj = String.valueOf(max_adj_close);
        String output = max_vol + "," + min_vol + "," + max_adj;
        outval.set(output);
        context.write(key, outval);
    }
}
