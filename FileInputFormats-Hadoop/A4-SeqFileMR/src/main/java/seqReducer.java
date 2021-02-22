import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class seqReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text imageFilePath = null;
        for (Text filePath : values){
            imageFilePath=filePath;
            break;
        }
        context.write(imageFilePath,key);
    }
}
