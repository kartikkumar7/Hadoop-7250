import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedStdTup implements Writable {

    private float median;
    private float stdDev;

    public float getStdDev() {
        return stdDev;
    }

    public void setStdDev(float stdDev) {
        this.stdDev = stdDev;
    }

    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(median);
        dataOutput.writeFloat(stdDev);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        median = dataInput.readFloat();
        stdDev = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "MedStdTup{" +
                "median=" + median +
                ", stdDev=" + stdDev +
                '}';
    }
}
