package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;


public class shareSongCount {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: uniqueListeners <input path> <output path>");
      System.exit(-1);
    }
    
	//Job Related Configurations
	Configuration conf = new Configuration();
	Job job = new Job(conf, "Full_Song_Count_Job");
	job.setJarByClass(shareSongCount.class); 
    
    
    //number of reducers set to 1
    job.setNumReduceTasks(1);
    
    //Provide paths to pick the input file for the job
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    
    
    //Provide paths to pick the output file for the job, and delete it if already present
	Path outputPath = new Path(args[1]);
	FileOutputFormat.setOutputPath(job, outputPath);
	outputPath.getFileSystem(conf).delete(outputPath, true);

    
    //To set the mapper of this job and there is no Reducer
    job.setMapperClass(shareSongCountMapper.class);
    job.setReducerClass(shareSongCountReducer.class);

    
     //set the input and output format class
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    //We set output key as NullWritable as we are not returning key  	
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    //execute the job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
