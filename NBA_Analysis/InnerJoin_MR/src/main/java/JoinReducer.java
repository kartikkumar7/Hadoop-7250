import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text EMPTY_TEXT = new Text("");
    private Text tmp = new Text();
    private ArrayList<Text> seasonList = new ArrayList<Text>();
    private ArrayList<Text> playerList = new ArrayList<Text>();
    private String joinType = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        joinType = context.getConfiguration().get("join.type");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        seasonList.clear();
        playerList.clear();

        while (values.iterator().hasNext()){
            tmp = values.iterator().next();
            if (tmp.charAt(0) == 'S'){
                seasonList.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.charAt(0)=='G'){
                playerList.add(new Text(tmp.toString().substring(1)));
            }
        }

        if (joinType.equalsIgnoreCase("inner")){
            if (!seasonList.isEmpty() && !playerList.isEmpty()){
                for (Text S: seasonList){
                    for (Text G: playerList){
                        context.write(S, G);
                    }
                }
            }
        }
    }
}
