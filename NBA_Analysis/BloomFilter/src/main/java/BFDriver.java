import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class BFDriver {

    public static class BFMapper extends Mapper<LongWritable, Text, NullWritable, BloomFilter>{
        private BloomFilter filter = new BloomFilter(1_000_000,7, Hash.MURMUR_HASH);
        private int count = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            String[] Tokens = input.split(",");

            if (!Tokens[0].equalsIgnoreCase("GAME_ID") && Tokens.length>=20){
                String name = Tokens[5];
                float pt3 = Float.parseFloat(Tokens[12]);
                float pt2 = Float.parseFloat(Tokens[9]) - Float.parseFloat(Tokens[12]);
                float pt1 = Float.parseFloat(Tokens[15]);

                float ptTotal = pt1*1 + pt2*2 + pt3*3;

                if (ptTotal >= 40){
                    Key filterKey = new Key(name.getBytes());
                    filter.add(filterKey);
                    count++;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), filter);
        }

    }


    public static class BFReducer extends Reducer<NullWritable, BloomFilter, NullWritable, NullWritable>{

        private static String FILTER_OUTPUT_FILE_CONF = "bloomfilter.output.file";

        private BloomFilter filter = new BloomFilter(1_000_000,7, Hash.MURMUR_HASH);

        @Override
        protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
            for (BloomFilter val: values){
                filter.or(val);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Path outputFilePath = new Path(context.getConfiguration().get(FILTER_OUTPUT_FILE_CONF));
            FileSystem fs = FileSystem.get(context.getConfiguration());

            try (FSDataOutputStream fsdos = fs.create(outputFilePath)){
                filter.write(fsdos);
            } catch (Exception e){
                throw new IOException("Error while writing bloom filter to file system", e);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "BloomFilterCreation");
        job.setJarByClass(BFDriver.class);

        Path inputPath = new Path(args[0]);
        Path filterFolder = new Path(args[1]);
        String filterOutput = args[1] + Path.SEPARATOR + "filter";

        job.getConfiguration().set(BFReducer.FILTER_OUTPUT_FILE_CONF, filterOutput);

        job.setMapperClass(BFMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BloomFilter.class);

        job.setReducerClass(BFReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, filterFolder);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
