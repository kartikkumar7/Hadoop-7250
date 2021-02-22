import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

public class PlayerMinutePair implements WritableComparable<PlayerMinutePair> {
    public String playerName;
    public float minutes;

    public PlayerMinutePair(){}

    public PlayerMinutePair(String name, float min){
        super();
        this.set(name, min);
    }

    public void set(String name, float min){
        this.playerName = (name==null)?"" : name;
        this.minutes = min;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(playerName);
        dataOutput.writeFloat(minutes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.playerName = dataInput.readUTF();
        this.minutes = dataInput.readFloat();
    }

    @Override
    public int compareTo(PlayerMinutePair playerMinutePair) {
        int nameCmp = playerName.toLowerCase().compareTo(playerMinutePair.playerName.toLowerCase());
        if (nameCmp!=0){
            return nameCmp;
        }
        else{
            return -1*Float.compare(minutes,playerMinutePair.minutes);
        }
    }
}
