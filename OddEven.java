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

public class OddEven{

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static Text even = new Text("even");
    private Text odd = new Text("odd");

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      int token;
      while (itr.hasMoreTokens()) {
        token = Integer.parseInt(itr.nextToken());
        if(token%2==0){
            context.write(even,new Text(Integer.toString(token)));
        }
        else{
            context.write(odd,new Text(Integer.toString(token)));
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text odd = new Text("odd");
    private Text even = new Text("even");
    private IntWritable one = new IntWritable(1);
    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String nums = "";
      if(key.equals(odd)==true){
        for(Text val:values){
            nums += " "+val.toString();
        }
        context.write(odd,new Text(nums));
      }
      else{
        for(Text val:values){
            nums += " "+val.toString();
        }
        context.write(even,new Text(nums));
      }
  }
 }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(OddEven.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);    
    FileInputFormat.addInputPath(job, new Path("/home/bdalab/Music/input.txt"));
    FileOutputFormat.setOutputPath(job, new Path("/home/bdalab/Music/EvenOdd/output"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
