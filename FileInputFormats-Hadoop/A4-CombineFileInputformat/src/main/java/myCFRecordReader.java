import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class myCFRecordReader extends RecordReader<LongWritable, Text> {
    private LineRecordReader lineRecordReader = new LineRecordReader();

    public myCFRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
        FileSplit fileSplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations());
        lineRecordReader.initialize(fileSplit, context);
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return lineRecordReader.getCurrentKey();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return lineRecordReader.nextKeyValue();
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return lineRecordReader.getCurrentValue();
    }
}
