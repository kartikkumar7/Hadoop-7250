import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myMapper extends Mapper<LongWritable, Text, Text, MinMaxStore> {
    private Text stock_symbol = new Text();
    private MinMaxStore output = new MinMaxStore();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] Tokens = value.toString().split(",");

        if(!Tokens[0].equalsIgnoreCase("exchange")){
            stock_symbol.set(Tokens[1]);
            output.setMax_volume(Float.parseFloat(Tokens[7]));
            output.setMin_volume(Float.parseFloat(Tokens[7]));
            output.setMax_price_adj(Float.parseFloat(Tokens[8]));
            context.write(stock_symbol, output);
        }

    }
}
