import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesTestMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

   @Override
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] fields = line.split("\t");
      if (fields.length > 2) {
         String store = fields[2];
         if (store.equals("Hialeah")) {
            String sValue = fields[4];
            float sales = Float.parseFloat(sValue);
            long salesValue = (long)(sales * 100);
            context.write(new Text(store), new LongWritable(salesValue));
         }
      }
   }
}
