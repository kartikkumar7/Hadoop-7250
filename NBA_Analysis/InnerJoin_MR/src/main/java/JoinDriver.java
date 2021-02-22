import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs =FileSystem.get(conf);
        if(fs.exists(new Path(args[2])))
        {
            fs.delete(new Path(args[2]), true);
        }
        Job job = Job.getInstance(conf);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, GDMapper.class);
        Path outputpath = new Path(args[2]);

        job.setReducerClass(JoinReducer.class);
        job.setJarByClass(JoinDriver.class);

        job.getConfiguration().set("join.type", "inner");
        FileOutputFormat.setOutputPath(job, outputpath);

        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


