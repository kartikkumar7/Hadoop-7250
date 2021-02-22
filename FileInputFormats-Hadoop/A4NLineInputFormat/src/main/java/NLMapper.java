import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable outval = new IntWritable();
    Text outkey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if(value.toString().length() > 0){
            String[] arrEmpAttributes = value.toString().split("\\t");
            outkey.set(arrEmpAttributes[0]);
            outval.set(Integer.parseInt(arrEmpAttributes[1]));
            context.write(outkey, outval);
        }
    }
}
