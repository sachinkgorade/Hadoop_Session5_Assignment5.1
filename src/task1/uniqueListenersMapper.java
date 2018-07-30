package task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class uniqueListenersMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  private final static IntWritable one = new IntWritable(1);	 

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {   
	  
	  // Here we are converting Text to String
	String content = value.toString();
	
	  String[] linesArray = content.split("  ");
	  
      for(String line : linesArray){
    	  
    	  //we are splitting line by pipe (|) 	  
          String[] word = line.split("\\|");

          //we are assigning listener values from word 
          Text listeners = new Text(word[0]);

		    context.write(listeners,one); 

          	  
	}
  }

}