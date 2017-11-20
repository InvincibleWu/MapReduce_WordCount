package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * This is the first version of the Assignment 1, which utilizes a combiner.
 * Writen by Xinhong Wu
 * ZID: z5089853
*/

public class WordAvgLen1 {
	
	/**
	 * Define a new class IntPair:
	 * 	two variables:
	 * 		'first' express the accumulative length
	 * 		'second' express the accumulative number of the word
	*/
	public static class IntPair implements Writable {
		private int first, second;
		public IntPair(){
		  
		}
		public IntPair(int first, int second){
		  set(first, second);
		}
		public void set(int left, int right){
		  first = left;
		  second = right;
		}
		public int getFirst(){
		  return first;
		}
		public int getSecond(){
		  return second;
		}
		public void write(DataOutput out) throws IOException{
		  out.writeInt(first);
		  out.writeInt(second);
		}
		public void readFields(DataInput in) throws IOException{
		  first = in.readInt();
		  second = in.readInt();
		}
	}



	/**
	 * This is the Mapper class
	 * The input: 
	 * 		key: index of one line
	 * 		value: the text in that line
	 * The output:
	 * 		key: alphabetical letter
	 *		value: the IntPair class which stored the total length and the total count.
	 * 
	 * The map function:
	 * 		1. receive a line of text. 
	 * 		2. split the text into tokens
	 * 		3. for each word begin with a alphabetical letter, convert it to a IntPair variable.
	 *		4. output
	 * 
	*/
  	public static class WordAvgLen1Mapper extends Mapper<Object, Text, Text, IntPair>{
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
	    	//System.out.println("mapper");
	    	while (itr.hasMoreTokens()) {
		    	String w = itr.nextToken();
		        int wordLength = w.length();
		        char firstLetter = w.charAt(0);
		        word.set(String.valueOf(firstLetter));
		        if ((firstLetter >= 'a') && (firstLetter <= 'z')){
		        	IntPair lengthAndCount = new IntPair(wordLength, 1);
		        	context.write(word, lengthAndCount);	
	        	}
	    	}
	    }
	}
  
    


    
	/**
	 * This is the combiner class
	 * The input:
	 * 		key: alphabetical letter
	 *		value: the IntPair class which stored the total length and the total count.
	 * The output:
	 * 		key: alphabetical letter
	 *		value: the IntPair class which stored the total length and the total count.
	 * 
	 * By using the combiner class, we can combine the output of mapper before it send to reducer process.
	 *
	*/

  	public static class WordAvgLen1Combiner extends Reducer<Text, IntPair, Text, IntPair>{
	  	private IntPair outputValue = new IntPair();
	  
	  	public void reduce(Text key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException {
		    //System.out.println("******************************");
		  	int sumLength = 0;
		  	int sumCount = 0;
			for (IntPair value : values){
				sumLength += value.getFirst();
				sumCount += value.getSecond();
			}

		  	outputValue.set(sumLength, sumCount);
		  	context.write(key, outputValue);
	  	}
  	}




	/**
	 * This is the reducer class
	 * The input:
	 * 		key: alphabetical letter
	 *		value: the IntPair class which stored the total length and the total count.
	 * The output:
	 * 		key: alphabetical letter
	 * 		value: the average length start with the alphabetical letter
	 *
	*/

  	public static class WordAvgLen1Reducer extends Reducer<Text,IntPair,Text,DoubleWritable> {
    	private DoubleWritable output = new DoubleWritable();

	    public void reduce(Text key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException {
	      	int sumLength = 0;
	      	int sumCount = 0;
	      	
	      	for (IntPair value : values) {
	        	sumLength += value.getFirst();
	        	sumCount += value.getSecond();
	      	}

	      	double avg = (double)sumLength/(double)sumCount;
	      
	      	output.set(avg);
	      	context.write(key, output);
	    }
  	}


	/**
	 * This is the main class
	*/

  	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "WordAvgLen1");
	    job.setJarByClass(WordAvgLen1.class);
	    job.setMapperClass(WordAvgLen1Mapper.class);
	    job.setCombinerClass(WordAvgLen1Combiner.class);
	    job.setReducerClass(WordAvgLen1Reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntPair.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
