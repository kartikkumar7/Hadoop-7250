import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GDWritable implements Writable {
    private String gameId;
    private String playerName;
    private float totalPts;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(gameId);
        dataOutput.writeUTF(playerName);
        dataOutput.writeFloat(totalPts);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        gameId = dataInput.readUTF();
        playerName = dataInput.readUTF();
        totalPts = dataInput.readFloat();
    }

    public String getGameId() {
        return gameId;
    }

    public void setGameId(String gameId) {
        this.gameId = gameId;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public float getTotalPts() {
        return totalPts;
    }

    public void setTotalPts(float totalPts) {
        this.totalPts = totalPts;
    }
}
