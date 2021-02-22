import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HomeAwayStore implements Writable {
    private float pts_diff;
    private float ast_diff;
    private float reb_diff;

    public float getPts_diff() {
        return pts_diff;
    }

    public void setPts_diff(float pts_diff) {
        this.pts_diff = pts_diff;
    }

    public float getAst_diff() {
        return ast_diff;
    }

    public void setAst_diff(float ast_diff) {
        this.ast_diff = ast_diff;
    }

    public float getReb_diff() {
        return reb_diff;
    }

    public void setReb_diff(float reb_diff) {
        this.reb_diff = reb_diff;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pts_diff = dataInput.readFloat();
        ast_diff = dataInput.readFloat();
        reb_diff = dataInput.readFloat();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(pts_diff);
        dataOutput.writeFloat(ast_diff);
        dataOutput.writeFloat(reb_diff);
    }

    @Override
    public String toString() {
        return "HomeAwayStore{" +
                "pts_diff=" + pts_diff +
                ", ast_diff=" + ast_diff +
                ", reb_diff=" + reb_diff +
                '}';
    }
}
