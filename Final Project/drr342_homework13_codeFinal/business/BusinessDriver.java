import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BusinessDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BusinessDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: BusinessDriver <in> <out>");
			System.exit(1);
		}

		Path inputPath = new Path(otherArgs[0]);
		Path outputPath = new Path(otherArgs[1]);

		Configuration businessJobConf = getConf();
		// businessJobConf.set("ID", "05");

		Job businessJob = Job.getInstance(businessJobConf, "Business Job");
		businessJob.setJarByClass(BusinessDriver.class);
		businessJob.setNumReduceTasks(1);
		businessJob.setMapperClass(BusinessMapper.class);
		businessJob.setReducerClass(BusinessReducer.class);
		businessJob.setMapOutputKeyClass(Text.class);
		businessJob.setMapOutputValueClass(IntWritable.class);
		businessJob.setOutputKeyClass(NullWritable.class);
		businessJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(businessJob, inputPath);
		FileOutputFormat.setOutputPath(businessJob, outputPath);
		return businessJob.waitForCompletion(true) ? 0 : 1;
	}

}
