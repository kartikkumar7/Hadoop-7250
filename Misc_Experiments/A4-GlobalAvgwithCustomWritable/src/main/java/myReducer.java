import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myReducer extends Reducer<Text, AvgStore, Text, AvgStore> {
    private AvgStore globalAvg = new AvgStore();

    @Override
    protected void reduce(Text key, Iterable<AvgStore> values, Context context) throws IOException, InterruptedException {
        float globalSum = 0;
        int globalCount = 0;
        for (AvgStore val: values){
            globalSum += val.getCount() * val.getLocal_avg();
            globalCount += val.getCount();
        }
        globalAvg.setCount(globalCount);
        globalAvg.setLocal_avg(globalSum/globalCount);
        context.write(key, globalAvg);
    }
}
