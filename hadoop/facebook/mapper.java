package facebook;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class fb_mapper extends Mapper<LongWritable, Text, IntWritable,  IntWritable>{
	
	@Override
    public void map(LongWritable key, Text value, Context context) throws 
     IOException, InterruptedException {
        String line = value.toString();
        String[] idArray = line.split("\\W+");
        String uniqueId = idArray[0];
        int count = 0;
        int idArraySize = idArray.length;
        count = idArraySize - 1;
        context.write(new IntWritable(count), new IntWritable(Integer.parseInt(uniqueId)));
        
     }





}



