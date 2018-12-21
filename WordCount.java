import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

  public static class Map extends Mapper<LongWritable, Text, Text, Text>{
	  public void map(LongWritable key, Text value, Context context)
			  throws IOException, InterruptedException {
   
	  	//Get FileName from reporter
		String filename =  ((FileSplit) context.getInputSplit()).getPath().getName().replace(".txt", "");
        
		String line = value.toString();
        Text keyres = new Text();
		StringTokenizer itr = new StringTokenizer(line);
		while (itr.hasMoreTokens()) {
			keyres.set(itr.nextToken());
			context.write(keyres, new Text(filename));
		}
	  }
  }
  public static class Reduce  extends Reducer<Text,Text,Text,Text> {

	    public void reduce(Text key, Iterable<Text> values, Context context) 
	    		throws IOException, InterruptedException {
	 
	      String docId = "";
	      for (Text val : values) {
	    	  	docId=docId+val.toString()+",";
	      }
	      docId=docId.substring(0, docId.length()-1);
	      String [] arr = docId.split(",");
	      
	      HashMap<String,Integer> reshash = new HashMap<String,Integer>();
	      reshash.put(arr[0], 1);
	      for(int i = 1;i<arr.length;i++){
	    	  	if (reshash.containsKey(arr[i])) {
	    	  		
	    	  		reshash.put(arr[i], reshash.get(arr[i])+1);
	          }else {
	        	  	reshash.put(arr[i], 1);
	          }
	      }
	      
	      Set<String> keyset = reshash.keySet();
	      Iterator<String> it = keyset.iterator();
	      String rel="";
	      while(it.hasNext()) {
	    	  	 String keyval=(String)it.next();
	    	  	 Integer valueval = (Integer)reshash.get(keyval);
	    	  	 rel=rel+"  "+keyval+":"+valueval;
	      }
	      context.write(key,new Text(rel));
	    }
 }
  
  public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
    
	Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);//main class
    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    Path outputPath = new Path(args[1]);
    // hadoop jar wordcout.jar /input /output
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));
    
    
    
    outputPath.getFileSystem(conf).delete(outputPath, true);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
