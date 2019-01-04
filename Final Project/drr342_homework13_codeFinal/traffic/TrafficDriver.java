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

public class TrafficDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TrafficDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: TrafficDriver <in> <out> <traffic2ca>");
			System.exit(1);
		}

        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path (otherArgs[1]);
        URI traffic2ca = new URI(otherArgs[2] + "#MAP");

		Configuration trafficJobConf = getConf();
//		trafficJobConf.set("ID", "04");

		Job trafficJob = Job.getInstance(trafficJobConf, "Traffic Job");
		trafficJob.addCacheFile(traffic2ca);
		trafficJob.setJarByClass(TrafficDriver.class);
		trafficJob.setNumReduceTasks(1);
		trafficJob.setMapperClass(TrafficMapper.class);
		trafficJob.setReducerClass(TrafficReducer.class);
		trafficJob.setMapOutputKeyClass(Text.class);
		trafficJob.setMapOutputValueClass(Text.class);
		trafficJob.setOutputKeyClass(NullWritable.class);
		trafficJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(trafficJob, inputPath);
		FileOutputFormat.setOutputPath(trafficJob, outputPath);
		return trafficJob.waitForCompletion(true) ? 0 : 1;
	}
	
}
