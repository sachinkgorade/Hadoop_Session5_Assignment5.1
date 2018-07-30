package task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashSet;


public class uniqueListenersReducer
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
    
   //we are using HashSet to have unique count of listeners      
      HashSet<Integer> set = new HashSet<Integer>();
      
      for (IntWritable value : values) {
    	  if(set.add(value.get()))
		sum+=value.get();	
       }		
      
  }
  
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new Text("Number of unique listeners"), new IntWritable(sum));
  }
  
}
