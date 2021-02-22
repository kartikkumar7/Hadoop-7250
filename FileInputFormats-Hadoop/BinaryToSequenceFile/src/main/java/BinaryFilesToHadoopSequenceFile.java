import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

public class BinaryFilesToHadoopSequenceFile {

    public static class BinaryFilesToHadoopSequenceFileMapper extends Mapper<Object, Text, Text, BytesWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String uri = value.toString();
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(new Path(uri));
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                byte buffer[] = new byte[1024 * 1024];

                while (in.read(buffer, 0, buffer.length) >= 0) {
                    bout.write(buffer);
                }
                context.write(value, new BytesWritable(bout.toByteArray()));
            } finally {
                IOUtils.closeStream(in);
            }
        }
    }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();

            Job job = new Job(conf, "BinaryFilesToHadoopSequenceFile");
            job.setJarByClass(BinaryFilesToHadoopSequenceFile.class);
            job.setMapperClass(BinaryFilesToHadoopSequenceFileMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true)?0:1);
        }
    }

