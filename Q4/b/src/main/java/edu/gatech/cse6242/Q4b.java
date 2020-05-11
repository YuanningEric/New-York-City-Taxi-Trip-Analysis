package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;



public class Q4b {

    /* TODO: Update variable below with your gtid */
    final String gtid = "yzheng403";

    public static class FareMapper
    extends Mapper<LongWritable, Text, IntWritable, FloatWritable>{

        @Override
        public void map(LongWritable offset, Text lineText, Context context)
        throws IOException,  InterruptedException{

          String line = lineText.toString();
          String[] tokens = line.split("\t");
          Integer PassengerCount = Integer.parseInt(tokens[2]);
          Float TotalFare = Float.parseFloat(tokens[3]);

          context.write(new IntWritable(PassengerCount),new FloatWritable(TotalFare));
         }
    }

    public static class FareReducer
        extends Reducer<IntWritable,FloatWritable, IntWritable, Text>{
            @Override public void reduce( IntWritable PassengerCount,  Iterable<FloatWritable> TotalFares,  Context context)
            throws IOException,  InterruptedException {

            Float fareSum = 0.0f;
            Integer count = 0;

            for ( FloatWritable TotalFare  : TotalFares) {
               fareSum += TotalFare.get();
               count += 1;
             }
            float avgFare = ((float)fareSum) / count;
            context.write(PassengerCount,  new Text (String.format("%,.02f", fareSum)));
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q4b");

    /* TODO: Needs to be implemented */
    job.setJarByClass(Q4b.class);
    job.setMapperClass(FareMapper.class);
    job.setReducerClass(FareReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(FloatWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
