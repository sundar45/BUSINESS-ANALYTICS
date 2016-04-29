package facebook;

	import java.io.IOException;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Reducer;


	
	public class fb_reducer extends Reducer<IntWritable, IntWritable, IntWritable,  IntWritable>{
	    static int userRank = 0;
	    static int friendCount = 30000000;
	    /*frnds, userid*/
	    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
	  			throws IOException, InterruptedException {
	        
	    	
	    	
	        for (IntWritable value: values){
	        	
	        	if ( friendCount !=  key.get() ){
	        		friendCount = key.get();
	        		userRank ++;
	        		
	        	}
	        	context.write(value, new IntWritable(userRank));
	        	
	        }
	        
	        
	    
	}
	   
	 }
	    #prasanna sundar MSBA;

	    
	    
	   
