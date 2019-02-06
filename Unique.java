import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Unique{

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private Text result = new Text("");
    private IntWritable one = new IntWritable(1);
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      context.write(key, one)   ;
  }
 }
  public static class KeyValueSwapper extends Mapper<Text,Text,Text,IntWritable>{
    Text output = new Text("unique");
    int freq = 0;
    @Override
    public void map(Text key, Text val, Context context) throws IOException,InterruptedException{
        context.write(output,new IntWritable(1));
    }  
  
  }
  public static class UniqueReducer extends Reducer <Text,IntWritable,Text,IntWritable>{
    private int sum = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context){
        for(IntWritable val:values){
            sum += val.get();
        }
    }
    public void cleanup(Context context) throws IOException, InterruptedException{
        context.write(new Text("Uniques: "),new IntWritable(sum));
        
    }
  
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Unique.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);    
    FileInputFormat.addInputPath(job, new Path("/home/bdalab/Music/input.txt"));
    FileOutputFormat.setOutputPath(job, new Path("out45" ));
    job.waitForCompletion(true);
    Job job1 = Job.getInstance(conf, "highest word");
    job1.setJarByClass(Unique.class);
    job1.setMapperClass(KeyValueSwapper.class);
    job1.setCombinerClass(UniqueReducer.class);
    job1.setReducerClass(UniqueReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setInputFormatClass(KeyValueTextInputFormat.class);    
    FileInputFormat.addInputPath(job1, new Path("out45"));
    FileOutputFormat.setOutputPath(job1, new Path("/home/bdalab/Music/Distint/output"));
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
}
