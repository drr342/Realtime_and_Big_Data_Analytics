import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DemographicsDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DemographicsDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: DemogrpahicsDriver <in> <out> <tract2ca>");
			System.exit(1);
		}

        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path (otherArgs[1]);
		URI mapURI = new URI(otherArgs[2] + "#MAP");
		
		Configuration demoJobConf = getConf();
//		demoJobConf.set("ID", "02");

		Job demoJob = Job.getInstance(demoJobConf, "Demographics Job");
		demoJob.addCacheFile(mapURI);
		demoJob.setJarByClass(DemographicsDriver.class);
		demoJob.setNumReduceTasks(1);
		demoJob.setMapperClass(DemographicsMapper.class);
		demoJob.setReducerClass(DemographicsReducer.class);
		demoJob.setMapOutputKeyClass(Text.class);
		demoJob.setMapOutputValueClass(Text.class);
		demoJob.setOutputKeyClass(NullWritable.class);
		demoJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(demoJob, inputPath);
		FileOutputFormat.setOutputPath(demoJob, outputPath);
		return demoJob.waitForCompletion(true) ? 0 : 1;
	}
}
