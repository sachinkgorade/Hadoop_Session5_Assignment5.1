package task3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class shareSongCountMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  private final static IntWritable one = new IntWritable(1);	 

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {   
	  
  
	String content = value.toString();
	
	  String[] linesArray = content.split("  ");
	  
      for(String line : linesArray){
    	  
    	 //we are splitting line by pipe (|) 
          String[] word = line.split("\\|");

          // Store 1st and 3rd column values
          Text song_shared = new Text(word[2]);
          Text listener = new Text(word[0]);
          
          // Select only those lines which have 3rd column value as "1"
             if(song_shared.equals(new Text("1")))          
		    context.write(listener,one);           	  
	}
  }

}