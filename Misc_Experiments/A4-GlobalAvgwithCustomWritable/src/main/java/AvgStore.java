import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgStore implements Writable {
    private int count = 0;
    private float local_avg=0;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public float getLocal_avg() {
        return local_avg;
    }

    public void setLocal_avg(float local_avg) {
        this.local_avg = local_avg;
    }

    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        local_avg = dataInput.readFloat();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeFloat(local_avg);
    }

    @Override
    public String toString() {
        return "AvgStore{" +
                "count=" + count +
                ", local_avg=" + local_avg +
                '}';
    }
}
