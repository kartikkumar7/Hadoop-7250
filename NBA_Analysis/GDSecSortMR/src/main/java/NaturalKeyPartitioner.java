import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<PlayerMinutePair, FloatWritable> {
    @Override
    public int getPartition(PlayerMinutePair playerMinutePair, FloatWritable floatWritable, int i) {
        return Math.abs(playerMinutePair.playerName.hashCode() & Integer.MAX_VALUE) % i;
    }
}
