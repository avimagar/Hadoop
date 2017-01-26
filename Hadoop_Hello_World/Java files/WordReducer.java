import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


/**
 *
 * @author avinash
 */



public class WordReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {


	public void reduce(Text key, Iterator<IntWritable> value,OutputCollector<Text, IntWritable> output, Reporter arg3)throws IOException 		{
		int cnt=0;
		while(value.hasNext()){
			IntWritable i=value.next();
			cnt+=i.get();
			
		}
		output.collect(key,new IntWritable(cnt));
	
	}

}
