
package crime;

// CrimeCount.java
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Crime 
{
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text finalword = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {

      String line = value.toString();
      String[] myarray = line.split(",");
      int endindex=2;
    //  word.set(myarray[4].substring(0,Integer.parseInt(endindex))+ myarray[5].substring(0,Integer.parseInt(endindex)));
      if(myarray.length>7)
      {
          if(!myarray[4].isEmpty()||!myarray[5].isEmpty())
          {
              if(myarray[5].isEmpty())
              {
               word.set(myarray[4].substring(0,endindex)+"NoLocation");
              }
              else if(myarray[4].isEmpty())
              {
                  word.set("NoLocation"+myarray[5].substring(0,endindex));
              }
              else
              {
                   word.set(myarray[4].substring(0,endindex)+ myarray[5].substring(0,endindex));
              }
          }
          else
          {
              word.set("NoLocationReported"+ "NoLocationReported");
          }
           finalword.set(word+myarray[7]);
      }
      else 
      {
          
          if(!myarray[4].isEmpty()||!myarray[5].isEmpty())
          {
              if(myarray[5].isEmpty())
              {
               word.set(myarray[4].substring(0,endindex)+"NoLocation");
              }
              else if(myarray[4].isEmpty())
              {
                  word.set("NoLocation"+myarray[5].substring(0,endindex));
              }
              else
              {
                   word.set(myarray[4].substring(0,endindex)+ myarray[5].substring(0,endindex));
              }
          }
          else
          {
              word.set("NoLocationReported"+ "NoLocationReported");
          }
           finalword.set(word+"NoCrimeType");
      }
      context.write(finalword, one);
      
    }
}
  
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
  {
   @Override
   public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException 
   {
      
      int sum = 0;
     for (IntWritable val : values) 
     {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
  }
  
  public static void main(String[] args) throws Exception 
  {
      
    Configuration conf = new Configuration(); 
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "crimecount");
    // conf.set("index", args[2]);
    //  System.out.println("Args 2:"+args[2]);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJarByClass(Crime.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
   // job.setNumReduceTasks(3);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    //job.waitForCompletion(true);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    
  }
}