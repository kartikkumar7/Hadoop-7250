import jdk.nashorn.internal.parser.Token;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GameMapper extends Mapper<LongWritable, Text, Text, HomeAwayStore> {

    private Text outkey = new Text();
//    private FloatWritable outval = new FloatWritable();
    private HomeAwayStore outval = new HomeAwayStore();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String Tokens[] = input.split(",");

        String pts_home = Tokens[7];
        String pts_away = Tokens[14];
        String ast_home = Tokens[11];
        String ast_away = Tokens[18];
        String reb_home = Tokens[12];
        String reb_away = Tokens[19];

        if ((!Tokens[0].equalsIgnoreCase("GAME_DATE_EST")) &&
                ((pts_home!= null) && (!pts_home.isEmpty()) && (pts_away!= null) && (!pts_away.isEmpty())) &&
                ((ast_home!= null) && (!ast_home.isEmpty()) && (ast_away!= null) && (!ast_away.isEmpty()))&&
                ((reb_home!= null) && (!reb_home.isEmpty()) && (reb_away!= null) && (!reb_away.isEmpty()))
        ){
            float home_pts = Float.valueOf(pts_home).floatValue();
            float away_pts = Float.valueOf(pts_away).floatValue();
            float home_ast = Float.valueOf(ast_home).floatValue();
            float away_ast = Float.valueOf(ast_away).floatValue();
            float home_reb = Float.valueOf(reb_home).floatValue();
            float away_reb = Float.valueOf(reb_away).floatValue();

            outval.setPts_diff(home_pts-away_pts);
            outval.setAst_diff(home_ast-away_ast);
            outval.setReb_diff(home_reb-away_reb);

            outkey.set(Tokens[5]);
            context.write(outkey, outval);
        }

//Count Home wins or season games
//        if ((!Tokens[0].equalsIgnoreCase("GAME_DATE_EST")) && !Tokens[7].equals("") && !Tokens[14].equals("")){
////            String season = Tokens[5];
//            float home_pts = Float.parseFloat(Tokens[7]);
//            float away_pts = Float.parseFloat(Tokens[14]);
//
//            if (home_pts>away_pts){
//                outkey.set("HomeWins");
//                outval.set(1);
//                context.write(outkey, outval);
//            }
////            outkey.set(season);
////            outval.set(1);
////            context.write(outkey, outval);
//        }



    }
}
