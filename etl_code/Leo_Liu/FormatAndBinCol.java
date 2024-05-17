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

// In this Mapreduce program, we will delete the header, some columns we do not need in our model, and the rows with non-numerical values in the sales columns. Also, we are going to add a binary column that tells whether the game is published in the 21st century. 

public class FormatAndBinCol {

  public static class FormatAndBinColMapper extends Mapper<Object, Text, Text, IntWritable> {

    private boolean headerPassed = false;

    private static boolean isNumeric(String str) {
      return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] row = value.toString().split(",");

      if (headerPassed) {

        if (isNumeric(row[3]) && isNumeric(row[6]) && isNumeric(row[7]) && isNumeric(row[8]) && isNumeric(row[9]) && isNumeric(row[10])) {

          if (Integer.parseInt(row[3]) >= 2000) {
            context.write(new Text(row[1] + "," + row[3] + "," + row[4] + "," + row[6] + "," + row[7] + "," + row[8] + "," + row[9] + "," + row[10] + "," + true), new IntWritable());
          } else {
            context.write(new Text(row[1] + "," + row[3] + "," + row[4] + "," + row[6] + "," + row[7] + "," + row[8] + "," + row[9] + "," + row[10] + "," + false), new IntWritable());
          }
        
        }
      
      } else {
        headerPassed = true;
      }

    }
  }

  public static class FormatAndBinColReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

      context.write(key, new IntWritable());

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "format and binary column");
    job.setJarByClass(FormatAndBinCol.class);
    job.setMapperClass(FormatAndBinColMapper.class);
    job.setReducerClass(FormatAndBinColReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}