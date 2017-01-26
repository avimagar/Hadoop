import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author avinash
 */


public class WordCount extends Configured implements Tool{


//	@Override from Tool Interface

	public int run(String[] arg0) throws Exception 
	{

//	Check for appropriate command line argument

		if(arg0.length<2)
		{
			System.out.println("Plz give appropriate directories");
			return -1;
		}
//	WordCount drives mapper & reducer classes therefore we require object of WordCount.class
		
		JobConf conf=new JobConf(WordCount.class);

//	Setting input file & output directory & deleting existing directory having same name as that of output directory 
		
		FileInputFormat.setInputPaths(conf, new Path(arg0[0]));
		FileSystem.get(conf).delete(new Path(arg0[1]),true);
		FileOutputFormat.setOutputPath(conf, new Path(arg0[1]));
		
//	Setting mapper &reducerfile

		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);

//	Specifying types of output key & output value of mapper 
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
//	Submitting the job
 
		JobClient.runJob(conf);
		return 0;
	}
	
	public static void main(String []args) throws Exception
	{

//	Checking for exit code
		
		int exitcode=ToolRunner.run(new WordCount(), args);

		System.exit(exitcode);
		
	}
	
	
}
 
