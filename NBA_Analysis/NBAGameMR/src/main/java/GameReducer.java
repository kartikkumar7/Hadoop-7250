import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GameReducer extends Reducer<Text, HomeAwayStore, Text, HomeAwayStore> {
//    private FloatWritable sumOfGames = new FloatWritable();
    private HomeAwayStore result= new HomeAwayStore();

    @Override
    protected void reduce(Text key, Iterable<HomeAwayStore> values, Context context) throws IOException, InterruptedException {
        int count =0;
        float pts_diff_sum =0;
        float ast_diff_sum =0;
        float reb_diff_sum =0;

        for (HomeAwayStore val: values){
            pts_diff_sum += val.getPts_diff();
            ast_diff_sum += val.getAst_diff();
            reb_diff_sum += val.getReb_diff();
            count++;
        }

        result.setPts_diff(pts_diff_sum/count);
        result.setAst_diff(ast_diff_sum/count);
        result.setReb_diff(reb_diff_sum/count);

        context.write(key, result);


//        int count =0;
//        for (FloatWritable val: values){
//            count += val.get();
//        }
//        sumOfGames.set(count);
//        context.write(key, sumOfGames);
    }
}
