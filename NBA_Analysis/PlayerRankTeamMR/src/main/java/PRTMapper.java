import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PRTMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outkey = new Text();
    private IntWritable outval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] Tokens = input.split(",");

        if(!Tokens[1].equalsIgnoreCase("TEAM_ID") & !Tokens[9].isEmpty()){
//            String conference = Tokens[4];
            String arena = Tokens[8];
            outkey.set(arena);
            outval.set(Integer.parseInt(Tokens[9]));
            context.write(outkey, outval);

        }
    }
}
