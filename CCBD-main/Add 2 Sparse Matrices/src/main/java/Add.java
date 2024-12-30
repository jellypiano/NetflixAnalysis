import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Triple implements Writable {
    public int i;
    public int j;
    public double value;
	
    Triple () {}
	
    Triple ( int i, int j, double v ) { this.i = i; this.j = j; value = v; }
	
    public void write ( DataOutput out ) throws IOException {
        out.writeInt(this.i);
        out.writeInt(this.j);
        out.writeDouble(this.value);
    }

    public void readFields ( DataInput in ) throws IOException {
        i=in.readInt();
        j=in.readInt();
        value=in.readDouble();
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j+"\t"+value;
    }
}

class Block implements Writable {
    final static int rows = 100;
    final static int columns = 100;
    public double[][] data;

    Block () {
        data = new double[rows][columns];
    }

    public void write ( DataOutput out ) throws IOException {
        		//try {
			//FileOutputStream fs = new FileOutputStream("test.dat");
			//ObjectOutputStream os = new ObjectOutputStream(fs);
			//os.writeObject(data); // 3
			//os.close();
		//} catch (Exception e) {
		//	e.printStackTrace();
		//} 
            for ( int i = 0; i < rows; i++ ) {
                for ( int j = 0; j < columns; j++ ){
                     out.writeDouble(data[i][j]);   
                }    
            }

    }

    public void readFields ( DataInput in ) throws IOException {
        for ( int i = 0; i < rows; i++ ) {
            for ( int j = 0; j < columns; j++ ){
                     data[i][j]= in.readDouble();   
                }    
            }       
    }

    @Override
    public String toString () {
        String s = "\n";
        for ( int i = 0; i < rows; i++ ) {
            for ( int j = 0; j < columns; j++ )
                s += String.format("\t%.3f",data[i][j]);
            s += "\n";
        }
        return s;
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}
	
    Pair ( int i, int j ) { this.i = i; this.j = j; }
	
    public void write ( DataOutput out ) throws IOException {
        out.writeInt(this.i);
        out.writeInt(this.j);
        //out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        i=in.readInt(); 
        j=in.readInt();
        
    }

    @Override
    public int compareTo ( Pair pair ) {
        if (i < pair.i) {
            return -1;
	    } 
        else if (i > pair.i) {
		    return 1;
	    } 
        else {
		    if (j < pair.j) {
			    return -1;
		    } 
		    else if (j > pair.j) {
			    return 1;
		    }
	    }
	    return 0;
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j;
    }
}

public class Add extends Configured implements Tool {
    final static int rows = Block.rows;
    final static int columns = Block.columns;

    /* ... */
    public static class Mapper1 extends Mapper<Object,Text,Pair,Triple>{
        @Override
        public void map ( Object key, Text line, Context context )
                        throws IOException, InterruptedException {

                Scanner scannedLine = new Scanner(line.toString()).useDelimiter(",");
                int i = scannedLine.nextInt();
                int j = scannedLine.nextInt();
                double v=scannedLine.nextDouble();
                context.write(new Pair(i/rows,j/columns),new Triple(i%rows,j%columns,v));     
                scannedLine.close();
                }                      
    }

    public static class Reducer1 extends Reducer<Pair,Triple,Pair,Block> {
        @Override
        //static Vector<Triple> m_matrix_val = new Vector<Triple>();
        public void reduce ( Pair pair, Iterable<Triple> triples, Context context )
                           throws IOException, InterruptedException {
            Block b=new Block();
            //int i,j=0;
            //double v=0.0;
            for (Triple t: triples)
            {   int i=t.i;
                int j=t.j;
                b.data[i][j]=t.value;                
            }
            context.write(pair,b);

        }
    }
        public static class Mapper11 extends Mapper<Object,Text,Pair,Triple>{
        @Override
        public void map ( Object key, Text line, Context context )
                        throws IOException, InterruptedException {

                Scanner scannedLine = new Scanner(line.toString()).useDelimiter(",");
                int i = scannedLine.nextInt();
                int j = scannedLine.nextInt();
                double v=scannedLine.nextDouble();
                context.write(new Pair(i/rows,j/columns),new Triple(i%rows,j%columns,v));     
                scannedLine.close();
                }                      
    }

    public static class Reducer11 extends Reducer<Pair,Triple,Pair,Block> {
        @Override
        //static Vector<Triple> m_matrix_val = new Vector<Triple>();
        public void reduce ( Pair pair, Iterable<Triple> triples, Context context )
                           throws IOException, InterruptedException {
            Block b=new Block();
            //int i,j=0;
            //double v=0.0;
            for (Triple t: triples)
            {   int i=t.i;
                int j=t.j;
                b.data[i][j]=t.value;                 
            }
            context.write(pair,b);
        }
    }
    
    public static class Mapper2 extends Mapper<Pair,Block,Pair,Block>{
        @Override
        public void map ( Pair pair, Block block, Context context )
                        throws IOException, InterruptedException {
                context.write(pair,block);
            }
        }
    
    public static class Mapper3 extends Mapper<Pair,Block,Pair,Block>{
        @Override
        public void map ( Pair pair, Block block, Context context )
                        throws IOException, InterruptedException {
                context.write(pair,block);
            }
        }

    public static class Reducer2 extends Reducer<Pair,Block,Pair,Block> {
        @Override
        public void reduce ( Pair pair, Iterable<Block> blocks, Context context )
                           throws IOException, InterruptedException {
            Block s=new Block();
            for (Block b:blocks){
                for ( int i = 0; i < rows; i++ ) {
                    for ( int j = 0; j < columns; j++ ){
                        s.data[i][j] += b.data[i][j];
                }            
            };     
        }
            context.write(pair,s);
        }
    }
    @Override
    public int run ( String[] args ) throws Exception {
        /* ... */
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
    	ToolRunner.run(new Configuration(),new Add(),args);
        Configuration conf = new Configuration();
        Job job_1 = Job.getInstance(conf,"Job_1");
		job_1.setJarByClass(Add.class);
		job_1.setMapOutputKeyClass(Pair.class);
		job_1.setMapOutputValueClass(Triple.class);
        System.out.println(args[0]);
		MultipleInputs.addInputPath(job_1, new Path(args[0]), TextInputFormat.class, Mapper1.class);
        //MultipleInputs.addInputPath(job_1, new Path(args[1]), TextInputFormat.class, Mapper1.class);
        job_1.setMapperClass(Mapper1.class);
		job_1.setReducerClass(Reducer1.class);
        	job_1.setOutputKeyClass(Pair.class);
		job_1.setOutputValueClass(Block.class);
        	job_1.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job_1, new Path(args[2]));
        //SequenceFileOutputFormat.setOutputPath(job_1, new Path(args[3]));
        	job_1.waitForCompletion(true);
        
        Configuration conf11 = new Configuration();
        Job job_11 = Job.getInstance(conf,"Job_11");
		job_11.setJarByClass(Add.class);
		job_11.setMapOutputKeyClass(Pair.class);
		job_11.setMapOutputValueClass(Triple.class);
		MultipleInputs.addInputPath(job_11, new Path(args[1]), TextInputFormat.class, Mapper11.class);
        //MultipleInputs.addInputPath(job_1, new Path(args[1]), TextInputFormat.class, Mapper1.class);
        job_11.setMapperClass(Mapper11.class);
		job_11.setReducerClass(Reducer11.class);
        	job_11.setOutputKeyClass(Pair.class);
		job_11.setOutputValueClass(Block.class);
        	job_11.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job_11, new Path(args[3]));
        //SequenceFileOutputFormat.setOutputPath(job_1, new Path(args[3]));
        	job_11.waitForCompletion(true);
		
		//Job_2 configurations
        Job job_2 = Job.getInstance(conf,"Job_2");
		job_2.setJarByClass(Add.class);
        MultipleInputs.addInputPath(job_2, new Path(args[2]), TextInputFormat.class, Mapper2.class);
        MultipleInputs.addInputPath(job_2, new Path(args[3]), TextInputFormat.class, Mapper3.class);
        job_2.setMapperClass(Mapper2.class);
        job_2.setMapperClass(Mapper3.class);
		job_2.setReducerClass(Reducer2.class);
		job_2.setMapOutputKeyClass(Pair.class);
		job_2.setMapOutputValueClass(Block.class);
		job_2.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job_2, new Path(args[2]));
		SequenceFileInputFormat.addInputPath(job_2, new Path(args[3]));
        SequenceFileOutputFormat.setOutputPath(job_2, new Path(args[4]));
		job_2.setOutputFormatClass(TextOutputFormat.class);
        	FileOutputFormat.setOutputPath(job_2, new Path(args[4]));
		job_2.waitForCompletion(true);
    }
}
