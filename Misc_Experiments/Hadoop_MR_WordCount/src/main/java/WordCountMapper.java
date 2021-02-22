import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] words = input.split(" ");
        IntWritable one = new IntWritable(1);
        for(String word : words)
        {
            word = word.toUpperCase();
            context.write(new Text(word), one);
        }
    }
}
