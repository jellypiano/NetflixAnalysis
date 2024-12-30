import java.io.*;
import java.util.*;
import java.util.Scanner;
import java.lang.String.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
 
public class Netflix {
    public static class Mapper1 extends Mapper<Object,Text,IntWritable,IntWritable>{
        @Override
        public void map ( Object key, Text line, Context context )
                        throws IOException, InterruptedException {

                Scanner scannedLine = new Scanner(line.toString()).useDelimiter(",");
                if(!((line.toString()).endsWith(":"))){
                    int user = scannedLine.nextInt();
                    int rating = scannedLine.nextInt();
                    context.write(new IntWritable(user),new IntWritable(rating));  
                    }   
                scannedLine.close();
                }                      
    }

    public static class Reducer1 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable user, Iterable<IntWritable> ratings, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0;
            for (IntWritable r: ratings) {
                sum += r.get();
                count++;
            };
            int avg10x=(int)(sum/count*10);
            context.write(user,new IntWritable(avg10x));
        }
    }
    
    public static class Mapper2 extends Mapper<Object,Text,IntWritable,IntWritable>{
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
                Scanner scannedLine = new Scanner(value.toString()).useDelimiter("\t");
                int user = scannedLine.nextInt();
                int rating = scannedLine.nextInt();
                context.write(new IntWritable(rating), new IntWritable(1));
                scannedLine.close();

            }
        }

    public static class Reducer2 extends Reducer<IntWritable,IntWritable,DoubleWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable rating, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v: values) {
                sum += v.get();
            };
            double ratingdouble=(double)(rating.get());
            context.write(new DoubleWritable(ratingdouble/10),new IntWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob1");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Netflix.class);
        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2 ,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2 ,new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}