package task3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashSet;


public class shareSongCountReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private int sum;

    @Override
    protected void setup(Context context) {
        sum = 0;
    }
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
      System.out.println("From The Reducer=>"+key) ;	
      
       for (IntWritable value : values) {
	 	sum+=value.get();	
       }		      
   
  }
  
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new Text("Number of times a song was shared"), new IntWritable(sum));
  }
  
}
