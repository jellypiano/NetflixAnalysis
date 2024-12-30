import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.Vector;


class Vertex implements Writable {
    public short tag;           // 0 for a graph vertex, 1 for a group number
    public long group;          // the group where this vertex belongs to
    public long VID;            // the vertex ID
    public Vector<Long> adjacent;    // the vertex neighbors
    Vertex(short tag,long group,long VID,Vector adjacent)
    {
        this.tag=tag;
        this.group=group;
        this.VID=VID;
        this.adjacent=adjacent;
    }
    Vertex()
    { 
    }
    Vertex(short tag,long group)
    {
        this.tag=tag;
        this.group=group;
        this.VID=0;
        this.adjacent=new Vector();
    } 
    public void write ( DataOutput out ) throws IOException {
        out.writeShort(this.tag);
        out.writeLong(this.group);
        out.writeLong(this.VID);
        out.writeInt(adjacent.size());
        for(int i=0;i<adjacent.size();i++)
        { 
            out.writeLong(adjacent.get(i));
        }
    }
    public void readFields ( DataInput in ) throws IOException {
        tag=in.readShort();
        group=in.readLong();
        VID=in.readLong();
        int len=in.readInt(); 
        Vector<Long> adj=new Vector();
        for(int i=0;i<len;i++)
        {
            long elem=in.readLong(); 
            adj.addElement(elem);
        }
        adjacent=adj;
        } 
}


public class Graph {
    public static class hxprakashMapper1 extends Mapper<Object,Text,LongWritable,Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
        throws IOException, InterruptedException {
            Vector<Long> v =new Vector();
            Scanner line_scanned = new Scanner(value.toString()).useDelimiter(",");
            int x=0;
            Integer group_zero=0;
            long vid=0;
            Integer vid_val=0;
            while(line_scanned.hasNext())
            {
                vid_val=line_scanned.nextInt();
                if(x==0)
                {
                    vid=vid_val.longValue();
                    x=1;
                }
                else
                {
                    long adj_vid=vid_val.longValue();
                    v.add(adj_vid);
                } 
            }
            // System.out.println();
            Vertex v1=new Vertex(group_zero.shortValue(),vid,vid,v); 
            context.write(new LongWritable(vid),v1);
            line_scanned.close();
            }
        }
    public static class hxprakashMapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex> { 
        public void map ( LongWritable key, Vertex v, Context context )
        throws IOException, InterruptedException { 
            context.write(new LongWritable(v.VID),v);
            int size_adj=v.adjacent.size();
            for(int i=0;i<size_adj;i++)
            {
                short group=1;
                context.write(new LongWritable(v.adjacent.get(i)),new Vertex(group,v.group));
            }
            }
        }
        public static long min(long val1,long val2)
        {
            if(val1<val2)
            {
                return val1;
            }
            else
            {
                return val2;
            }
        }
    public static class hxprakashReducer2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        public void reduce ( LongWritable vid, Iterable<Vertex> values, Context context)
        throws IOException, InterruptedException {
            long m=Long.MAX_VALUE;
            Vector<Long> adj =new Vector();
            for(Vertex v:values) 
            {
                if(v.tag==0)
                {
                    adj=v.adjacent;
                }
                m=min(m,v.group);
            }
            short group_tag=0;
            context.write(new LongWritable(m),new Vertex(group_tag,m,vid.get(),adj));
            }
    }
    public static class hxprakashMapper3 extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> { 
        public void map ( LongWritable group, Vertex v, Context context )
        throws IOException, InterruptedException {
            context.write(group,new LongWritable(1));
        }
    }
    public static class hxprakashReducer3 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        public void reduce ( LongWritable group, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
            long m=0;
            for(LongWritable v:values) 
            {
                m=m+v.get();
            } 
            context.write(group,new LongWritable(m));
        }
    }
    public static void main ( String[] args ) throws Exception {
            Job Job1 = Job.getInstance();
            Job1.setJobName("Job1");
            Job1.setJarByClass(Graph.class);
            Job1.setOutputKeyClass(LongWritable.class);
            Job1.setOutputValueClass(Vertex.class);
            Job1.setMapOutputKeyClass(LongWritable.class);
            Job1.setMapOutputValueClass(Vertex.class);
            Job1.setMapperClass(hxprakashMapper1.class);
            Job1.setInputFormatClass(TextInputFormat.class);
            Job1.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(Job1,new Path(args[0]));
            FileOutputFormat.setOutputPath(Job1,new Path(args[1]+"/f0"));
            Job1.waitForCompletion(true);

            for ( short i = 0; i < 5; i++ ) {
                Job Job2 = Job.getInstance();
                Job2.setJobName("Job2");
                Job2.setJarByClass(Graph.class);
                Job2.setOutputKeyClass(LongWritable.class);
                Job2.setOutputValueClass(Vertex.class);
                Job2.setMapOutputKeyClass(LongWritable.class);
                Job2.setMapOutputValueClass(Vertex.class);
                Job2.setMapperClass(hxprakashMapper2.class);
                Job2.setReducerClass(hxprakashReducer2.class);
                Job2.setInputFormatClass(SequenceFileInputFormat.class);
                Job2.setOutputFormatClass(SequenceFileOutputFormat.class);
                FileInputFormat.setInputPaths(Job2,new Path(args[1]+"/f"+i));
                FileOutputFormat.setOutputPath(Job2,new Path(args[1]+"/f"+(i+1)));
                Job2.waitForCompletion(true);
            }
            
            Job Job3 = Job.getInstance();
            Job3.setJobName("Job3");
            Job3.setJarByClass(Graph.class);
            Job3.setOutputKeyClass(LongWritable.class);
            Job3.setOutputValueClass(Vertex.class);
            Job3.setMapOutputKeyClass(LongWritable.class);
            Job3.setMapOutputValueClass(LongWritable.class);
            Job3.setMapperClass(hxprakashMapper3.class);
            Job3.setReducerClass(hxprakashReducer3.class);
            Job3.setInputFormatClass(SequenceFileInputFormat.class);
            Job3.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(Job3,new Path(args[1]+"/f5"));
            FileOutputFormat.setOutputPath(Job3,new Path(args[2]));
            Job3.waitForCompletion(true);
            }
}

    /* ... */

//     public static void main ( String[] args ) throws Exception {
//         Job job = Job.getInstance();
//         job.setJobName("Graph");
//         /* ... First Map-Reduce job to read the graph */
//         job.waitForCompletion(true);
//         for ( short i = 0; i < 5; i++ ) {
//             job = Job.getInstance();
//             /* ... Second Map-Reduce job to propagate the group number */
//             job.waitForCompletion(true);
//         }
//         job = Job.getInstance();
//         /* ... Final Map-Reduce job to calculate the connected component sizes */
//         job.waitForCompletion(true);
//     }
// }
