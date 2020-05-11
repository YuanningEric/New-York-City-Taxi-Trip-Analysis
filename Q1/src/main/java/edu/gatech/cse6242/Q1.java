package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class Q1 {

/* TODO: Update variable below with your gtid */
  final String gtid = "yzheng403";

  public static class FareMapper
    extends Mapper<LongWritable, Text, Text, FloatWritable>{
    //private final static IntWritable one = new IntWritable(1);
    @Override
    public void map(LongWritable offset, Text lineText, Context context)
    throws IOException,  InterruptedException{
  String line = lineText.toString();
  String[] tokens = line.split(",");
  String PickUpID = tokens[0];

  try{
  float fare = Float.parseFloat(tokens[3]);

  if (PickUpID!=null && !PickUpID.isEmpty() && tokens[2]!="0" && fare > 0 ){
    context.write(new Text(PickUpID),new FloatWritable(fare));
    }
} catch(NumberFormatException e){System.out.println("fare can not be parsed");}

 }
}
    public static class FareReducer
        extends Reducer<Text,FloatWritable, Text, Text>{
            @Override public void reduce( Text PickUpID,  Iterable<FloatWritable> fares,  Context context)
            throws IOException,  InterruptedException {

            Integer count = 0;
            Float fareSum = 0.0f;

            for ( FloatWritable fare  : fares) {
               fareSum += fare.get();
               count+=1;
             }

             context.write(PickUpID,  new Text(Integer.toString(count) + "," + String.format("%,.02f", fareSum)));
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    /* TODO: Needs to be implemented */
    job.setJarByClass(Q1.class);
    job.setMapperClass(FareMapper.class);
    job.setReducerClass(FareReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
