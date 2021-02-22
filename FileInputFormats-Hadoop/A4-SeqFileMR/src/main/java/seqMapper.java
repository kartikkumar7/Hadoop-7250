import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class seqMapper extends Mapper<Text, BytesWritable, Text, Text>{
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        String md5Str;
        try{

            md5Str = calculateMd5(value.getBytes());
        } catch(NoSuchAlgorithmException e){
            e.printStackTrace();
            context.setStatus("Internal Error - cant find algorithm to calculate md5");
            return;
        }
        Text md5Text = new Text(md5Str);
        context.write(md5Text, key);
    }

    static String calculateMd5(byte[] imageData) throws NoSuchAlgorithmException{
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(imageData);
        byte[] hash = md.digest();

        String hexstring = new String();
        for (int i=0; i< hash.length; i++){
            hexstring += Integer.toString((hash[i] & 0xff) + 0x100,16).substring(1);

        }
        return hexstring;
    }
}
