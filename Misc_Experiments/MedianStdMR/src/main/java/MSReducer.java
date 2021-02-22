import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class MSReducer extends Reducer<Text, FloatWritable, Text, MedStdTup> {

    private MedStdTup result = new MedStdTup();
    private ArrayList<Float> away_pct_list = new ArrayList<Float>();

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        float count =0;

        away_pct_list.clear();

        result.setStdDev(0);

        for (FloatWritable val: values){
            away_pct_list.add(val.get());
            sum += val.get();
            ++count;
        }

        Collections.sort(away_pct_list);

        if (count % 2 == 0){
            result.setMedian((away_pct_list.get((int) count/2 -1) + away_pct_list.get((int) count/2)) / 2.0f);
        } else {
            result.setMedian(away_pct_list.get((int) count/2));
        }


        float mean = sum / count;
        float sumOfSquares = 0.0f;

        for (Float f: away_pct_list){
            sumOfSquares += (f - mean)* (f - mean);
        }

        result.setStdDev((float) Math.sqrt(sumOfSquares / (count-1)));
        context.write(key, result);
    }
}
