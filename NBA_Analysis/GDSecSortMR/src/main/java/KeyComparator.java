import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {

    public KeyComparator(){
        super(PlayerMinutePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PlayerMinutePair key1 = (PlayerMinutePair) a;
        PlayerMinutePair key2 = (PlayerMinutePair) b;

        int nameCmp = key1.playerName.toLowerCase().compareTo(key2.playerName.toLowerCase());
        if(nameCmp!=0){
            return nameCmp;
        }
        else{
            return -1*Float.compare(key1.minutes, key2.minutes);
        }
    }
}
