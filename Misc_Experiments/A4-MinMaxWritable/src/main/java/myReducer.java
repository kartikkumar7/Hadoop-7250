import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myReducer extends Reducer<Text, MinMaxStore, Text, MinMaxStore> {

    private MinMaxStore result = new MinMaxStore();

    @Override
    protected void reduce(Text key, Iterable<MinMaxStore> values, Context context) throws IOException, InterruptedException {
        result.setMax_volume(0);
        result.setMin_volume(0);
        result.setMax_price_adj(0);

        for (MinMaxStore val: values){
            if (result.getMax_volume() == 0 || val.getMax_volume()>result.getMax_volume()){
                result.setMax_volume(val.getMax_volume());
            }

            if (result.getMin_volume() == 0 || val.getMin_volume()<result.getMin_volume()){
                result.setMin_volume(val.getMin_volume());
            }

            if (result.getMax_price_adj() == 0 || val.getMax_price_adj()> result.getMax_price_adj()){
                result.setMax_price_adj(val.getMax_price_adj());
            }
        }

        context.write(key, result);
    }
}
