import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesTestMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

   @Override
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] fields = line.split("\t");
      if (fields.length > 2) {
         String store = fields[2];
         if (store.equals("Fort Worth")) {
            String sValue = fields[4];
            float sales = Float.parseFloat(sValue);
            context.write(new Text(store), new FloatWritable(sales));
            System.out.println("Test: " + sales);
         }
      }
   }
}
