import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Locale;

public class NaturalKeyComparator extends WritableComparator {
    public NaturalKeyComparator(){
        super(PlayerMinutePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PlayerMinutePair key1 = (PlayerMinutePair) a;
        PlayerMinutePair key2 = (PlayerMinutePair) b;

        return key1.playerName.toLowerCase().compareTo(key2.playerName.toLowerCase());
    }
}
