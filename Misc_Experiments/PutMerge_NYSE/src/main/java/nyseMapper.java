import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class nyseMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    Text stock_symbol = new Text();
    FloatWritable high_price = new FloatWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] Tokens = value.toString().split(",");
        //Tokens[1] = symbol & Tokens[4] = high_price
        if(!Tokens[0].equalsIgnoreCase("exchange")){
            stock_symbol.set(Tokens[1]);
            high_price.set(Float.parseFloat(Tokens[4]));
            context.write(stock_symbol, high_price);
        }
    }
}
