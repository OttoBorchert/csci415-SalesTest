import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.text.DecimalFormat;

public class SalesTestReducer extends Reducer<Text, LongWritable, Text, Text> {

   @Override
   public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      long totalValue = 0;
      for (LongWritable value : values) {
         totalValue += value.get();
      }
      Text t = new Text(String.format("%.2f", (double)totalValue/100.0));
      context.write(key, t);
   }
}

