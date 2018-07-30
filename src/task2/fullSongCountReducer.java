package task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class fullSongCountReducer
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
           
       for (IntWritable value : values) {
	 	sum+=value.get();	
       }		      
     
  }
  
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new Text("number of times a song was heard fully"), new IntWritable(sum));
  }
  
}
