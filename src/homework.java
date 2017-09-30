# homework


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  static int instanceCounter = 0;
  int counter = 0;

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
     private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
                Text temp = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String cleanLine = value.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " "); // avoid punctuation marks and other special characters
            String stop_words[];
            stop_words = new String[] {"a","an","the","and","is",".",","};
            chap_word = new String[] {"CHAPTER"};
            StringTokenizer itr = new StringTokenizer(cleanLine);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().trim()); // to avoid white spaces
                if(!(word.equals(chap_word)))
                {
                	for(int i =0; i < stop_words.length; i++)
                 {
                  
                  if(!(word.equals(stop_words[i])))
                  {             
                   temp = word;
                  }
                 
                 }
                 context.write(temp, one); //replace the one with the counter variable
                }
                else
                {
                	//counter++ add a global counter here and increment here
                  WordCount(){
                     instanceCounter++;
                     counter = instanceCounter;
                  }
                }
            }
      }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
    private IntWritable result = new IntWritable();
    private Map countMap = new HashMap<>();

    public void reduce(Text key, Iterable <IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) 
      {
        sum += val.get();
      }
      if (sum > 100)
        {
      countMap.put(key, new IntWritable(sum));
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

      Map sortedMap = sortByValues(countMap);
            int counter = 0;
            for (Text key: sortedMap.keySet()) {
                if (counter ++ == 50) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
      }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
