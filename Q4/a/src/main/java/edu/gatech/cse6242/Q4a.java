package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class Q4a {

    /* TODO: Update variable below with your gtid */
    final String gtid = "yzheng403";

    public static class DegreeMapper
    extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable minusOne = new IntWritable(-1);

        @Override
        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,  InterruptedException{


          String line = lineText.toString();
          String[] tokens = line.split("\t");
          Integer pickUpID = Integer.parseInt(tokens[0]);
          Integer dropOffID = Integer.parseInt(tokens[1]);

          context.write(new IntWritable(pickUpID),one);
          context.write(new IntWritable(dropOffID),minusOne);
         }
    }

    public static class DiffMapper
    extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,  InterruptedException{
          String line = lineText.toString();
          String[] tokens = line.split("\t");
          Integer diff = Integer.parseInt(tokens[1]);
          context.write(new IntWritable(diff),one);
         }
    }

    public static class DegreeReducer
        extends Reducer<IntWritable,IntWritable, IntWritable, IntWritable>{
            @Override public void reduce( IntWritable LocationID,  Iterable<IntWritable> degrees,  Context context)
            throws IOException,  InterruptedException {

            Integer degreeSum = 0;

            for ( IntWritable degree  : degrees) {
               degreeSum += degree.get();
             }

             context.write(LocationID,  new IntWritable (degreeSum));
        }
    }

    public static class DiffReducer
        extends Reducer<IntWritable,IntWritable, IntWritable, IntWritable>{
            @Override public void reduce( IntWritable diff,  Iterable<IntWritable> counts,  Context context)
            throws IOException,  InterruptedException {

            Integer sumCount = 0;

            for ( IntWritable count  : counts) {
               sumCount += count.get();
             }
             context.write(diff,  new IntWritable (sumCount));
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "get diff");
    //Job job = Job.getInstance(conf, "get diff");
    job.setJarByClass(Q4a.class);
    job.setMapperClass(DegreeMapper.class);
    job.setReducerClass(DegreeReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("/opt/"));
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
    job.waitForCompletion(true);

    Job job2 = new Job(conf, "get count of diff");
    //Job job2 = Job.getInstance(conf, "get count of diff");
    job2.setJarByClass(Q4a.class);
    job2.setMapperClass(DiffMapper.class);
    job2.setReducerClass(DiffReducer.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("/opt/"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    //System.exit(job2.waitForCompletion(true) ? 0 : 1);
    job2.waitForCompletion(true); 
  }
}
