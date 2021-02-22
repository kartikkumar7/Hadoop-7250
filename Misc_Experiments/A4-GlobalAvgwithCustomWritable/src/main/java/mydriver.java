import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class mydriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]), true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(mydriver.class);
        job.setMapperClass(myMapper.class);
        job.setReducerClass(myReducer.class);
        job.setCombinerClass(myReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgStore.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvgStore.class);

        job.setInputFormatClass(TextInputFormat.class);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
