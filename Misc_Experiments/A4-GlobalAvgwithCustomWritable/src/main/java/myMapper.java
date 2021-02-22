import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myMapper extends Mapper<LongWritable, Text, Text, AvgStore> {
    private Text outkey = new Text();
    private  AvgStore outval = new AvgStore();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] Tokens = value.toString().split(",");
        int count = 0;
        int sum = 0;
        if (!Tokens[0].equalsIgnoreCase("exchange")){
            String[] date = Tokens[2].split("-");

            outkey.set(date[0]);
            count +=1;
            outval.setCount(count);
            sum += Float.parseFloat(Tokens[8]);
            outval.setLocal_avg(sum/count);
            context.write(outkey, outval);
        }


    }
}
