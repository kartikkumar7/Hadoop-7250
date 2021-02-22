import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class GDDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs =FileSystem.get(conf);
        if(fs.exists(new Path(args[1])))
        {
            fs.delete(new Path(args[1]), true);
        }
        Job job = Job.getInstance(conf);
        job.setMapperClass(GDMapper.class);
        job.setReducerClass(GDReducer.class);
//        job.setCombinerClass(GDReducer.class);
        job.setJarByClass(GDDriver.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(FloatWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(FloatWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
