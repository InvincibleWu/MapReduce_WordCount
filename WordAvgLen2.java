package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
 * This is the second version of the Assignment 1, which utilizes the 'in-mapper combining'
 * Writen by Xinhong Wu
 * ZID: z5089853
*/

public class WordAvgLen2 {
  

    /**
     * Define a new class IntPair:
     *  two variables:
     *    'first' express the accumulative length
     *    'second' express the accumulative number of the word
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
        public void add(int length, int count){
            first += length;
            second += count;
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
     *    key: index of one line
     *    value: the text in that line
     * The output:
     *    key: alphabetical letter
     *    value: the IntPair class which stored the total length and the total count.
     * 
     * The map process:
     *    1. receive a line of text. 
     *    2. split the text into tokens
     *    3. for each word begin with a alphabetical letter, convert it to a IntPair variable.
     *    4. store the alphabetical letter and IntPair in the Hashmap
     *    5. after processed all the word in the line, combine the alphabetical letter and IntPair in the Hashmap
     *    6. output   
     * 
    */
    public static class WordAvgLen2Mapper extends Mapper<Object, Text, Text, IntPair>{
        private Map<String, IntPair> map;
      
        public Map<String, IntPair> getMap(){
            if(null == map){
            map = new HashMap<String, IntPair>();
            }
        return map;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
            Map<String, IntPair> map = getMap();
            while (itr.hasMoreTokens()) {
                String w = itr.nextToken();
                int wordLength = w.length();
                char k = w.charAt(0);
                String firstLetter = k + "";
            
                if (( k >= 'a') && (k <= 'z')){
                    if(map.containsKey(firstLetter)){
                        IntPair lengthAndCount = map.get(firstLetter);
                        lengthAndCount.add(wordLength, 1);
                        map.put(firstLetter, lengthAndCount);
                    } else{
                        IntPair lengthAndCount = new IntPair(wordLength, 1);
                        map.put(firstLetter, lengthAndCount);
                    }
                }
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException{
            Map<String, IntPair> map = getMap();
            Iterator<Map.Entry<String, IntPair>> e = map.entrySet().iterator();
            while(e.hasNext()){
                Map.Entry<String, IntPair> element = e.next();
                String k = element.getKey();
                IntPair v = element.getValue();
                context.write(new Text(k), v);
            }  
        }
    }
  
  
    /**
     * This is the reducer class
     * The input:
     *    key: alphabetical letter
     *    value: the IntPair class which stored the total length and the total count.
     * The output:
     *    key: alphabetical letter
     *    value: the average length start with the alphabetical letter
     *
    */  
    public static class WordAvgLen2Reducer extends Reducer<Text,IntPair,Text,DoubleWritable> {
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
        Job job = Job.getInstance(conf, "WordAvgLen2");
        job.setJarByClass(WordAvgLen2.class);
        job.setMapperClass(WordAvgLen2Mapper.class);
        //job.setCombinerClass(Combiner.class);
        job.setReducerClass(WordAvgLen2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntPair.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}