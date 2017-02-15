package com.baidumusic.evan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URISyntaxException;

public class MergeAllScore {
	private static final String TAB = "\t";

	private static class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			InputSplit inputSplit = (InputSplit) context.getInputSplit();
			String pathName = ((FileSplit) inputSplit).getPath().toString();
			String line = value.toString();
			if (pathName.contains("GetSongRelatedScore")) {
				String[] lParts = line.split(TAB);
				if (2 == lParts.length) {
					context.write(new Text(lParts[0]), new Text("MUSIC###" + lParts[1]));
				}
			} else if (pathName.contains("GetUserHabitScore")) {
				String[] lParts = line.split(TAB);
				if (2 == lParts.length) {
					context.write(new Text(lParts[0]), new Text("HABIT###" + lParts[1]));
				}
			}
		}
	}

	private static class MergeReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String music = "";
			String habit = "";
			for (Text val : values) {
				String sVal = val.toString();
				if (sVal.startsWith("MUSIC###")) {
					music = sVal.split("###")[1];
				} else if (sVal.startsWith("HABIT###")) {
					habit = sVal.split("###")[1];
				}
			}
			String res = "";
			if (music.length() == 0) {
				res = habit;
			} else if (habit.length() == 0) {
				res = music;
			} else {
				res = music + "," + habit;
			}
			context.write(key, new Text(res));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(MergeAllScore.class);
		job.setJobName("Evan_MergeAllScore");
		job.setNumReduceTasks(10);
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReduce.class);
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();

		if (args.length == 0) {
			System.err.println("Usage: class <in>");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);

		FileSystem hdfs = FileSystem.get(conf);
		String out = "/user/work/evan/tmp/MergeAllScore";
		Path path = new Path(out);
		hdfs.delete(path, true);

		MergeAllScore.runLoadMapReducue(conf, args[0], new Path(out));
	}
}