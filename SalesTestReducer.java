import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.text.DecimalFormat;

public class SalesTestReducer extends Reducer<Text, FloatWritable, Text, Text> {

   @Override
   public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
      float totalValue = 0.0f;
      for (FloatWritable value : values) {
         totalValue += value.get();
      }
      Text t = new Text(String.format("%.2f", totalValue));
      context.write(key, t);
   }
}

