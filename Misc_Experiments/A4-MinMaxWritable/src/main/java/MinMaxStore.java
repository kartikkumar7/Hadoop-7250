import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxStore implements Writable {
    private float max_volume;
    private float min_volume;
    private float max_price_adj;

    public float getMax_volume() {
        return max_volume;
    }

    public void setMax_volume(float max_volume) {
        this.max_volume = max_volume;
    }

    public float getMin_volume() {
        return min_volume;
    }

    public void setMin_volume(float min_volume) {
        this.min_volume = min_volume;
    }

    public float getMax_price_adj() {
        return max_price_adj;
    }

    public void setMax_price_adj(float max_price_adj) {
        this.max_price_adj = max_price_adj;
    }

    public void readFields(DataInput dataInput) throws IOException {
        max_volume = dataInput.readFloat();
        min_volume = dataInput.readFloat();
        max_price_adj = dataInput.readFloat();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(max_volume);
        dataOutput.writeFloat(min_volume);
        dataOutput.writeFloat(max_price_adj);
    }

    @Override
    public String toString() {
        return "MinMaxStore{" +
                "max_volume=" + max_volume +
                ", min_volume=" + min_volume +
                ", max_price_adj=" + max_price_adj +
                '}';
    }
}
