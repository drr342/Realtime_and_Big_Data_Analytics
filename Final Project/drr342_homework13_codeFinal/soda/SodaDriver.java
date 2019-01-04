import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SodaDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SodaDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err.println("Usage: SodaDriver <in> <out> <lib> <wait>");
			System.exit(1);
		}

        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path (otherArgs[1]);
		Path libPath = new Path(otherArgs[2]);
		boolean wait = otherArgs[3].toLowerCase().startsWith("t");

		Configuration sodaJobConf = getConf();
		sodaJobConf.setLong(FileInputFormat.SPLIT_MAXSIZE, 1024 * 1024);

		Job sodaJob = Job.getInstance(sodaJobConf, "Soda Job");

		FileSystem fs = FileSystem.get(sodaJobConf);
		FileStatus[] files = fs.listStatus(libPath);
		for (FileStatus file: files)
			sodaJob.addFileToClassPath(Path.getPathWithoutSchemeAndAuthority(file.getPath()));

		sodaJob.setJarByClass(SodaDriver.class);
		sodaJob.setNumReduceTasks(1);
		sodaJob.setMapperClass(SodaMapper.class);
		sodaJob.setReducerClass(SodaReducer.class);
		sodaJob.setOutputKeyClass(NullWritable.class);
		sodaJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(sodaJob, inputPath);
		FileOutputFormat.setOutputPath(sodaJob, outputPath);

		if (wait)
			return sodaJob.waitForCompletion(true) ? 0 : 1;
		else
			sodaJob.submit();
			return 0;
	}
}
