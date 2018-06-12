import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SolverCompare {

	public static class SolverMapper extends Mapper<Object, Text, Text, DoubleWritable>{
	
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException{
			String solverRecord = values.toString();
			String[] solverDetailsLine = solverRecord.split("\t");
			for(int i=1; i<solverDetailsLine.length; i++) {
				//String[] solverDetails = solverDetailsLine[i].split("\t");
				if(!solverDetailsLine[11].contains("Real") && solverDetailsLine[14].contains("solved") )
					context.write(new Text(solverDetailsLine[0]), new DoubleWritable(Double.parseDouble(solverDetailsLine[11])));
			}
		}
	}
	
	public static class SolverReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		private DoubleWritable runTime = new DoubleWritable();
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		
			for(DoubleWritable value: values) {
				this.runTime = value;
				context.write(key, this.runTime);
			}
			
		}
	}
	
//	public static class SolverAggregateReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
//		
//		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
//			
//		}
//	}
	
	public static void main(String[] args) throws Exception {
		   Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Solver Comparizion");
		    job.setJarByClass(SolverCompare.class);
		    job.setMapperClass(SolverMapper.class);
		    job.setCombinerClass(SolverReduce.class);
		    job.setReducerClass(SolverReduce.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    job.setNumReduceTasks(0);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}
