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

public class MergeFMDownoadVisitUGC {
	private static final String TAB = "\t";

	private static class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			InputSplit inputSplit = (InputSplit) context.getInputSplit();
			String pathName = ((FileSplit) inputSplit).getPath().toString();
			String line = value.toString();
			if (pathName.contains("ReadFromHiveUserVisitRecord")) {
				// FFF7BC8FA86FE5B8AD3332E1BAD6158C no-visit {"2016-12-08 15:39:28":"2016-12-08 15:39:28\t220.177.76.32"} no-sug
				int idx = line.indexOf(TAB);
				if (idx > 0 && idx < line.length()) {
					context.write(new Text(line.substring(0, idx)), new Text("VISIT###" + line.substring(idx + 1)));
				}
			} else if (pathName.contains("ReadFromUDWFMRecord")) {
				// F854B61E90E9D2253C6417101E1E5D18 92279724 {"2016-11-07
				// 10:13:12":"2016-11-07 10:13:12\t14.153.212.136"} no-del
				int idx = line.indexOf(TAB);
				if (idx > 0 && idx < line.length()) {
					context.write(new Text(line.substring(0, idx)), new Text("FM###" + line.substring(idx + 1)));
				}
			} else if (pathName.contains("ReadFromUDWDownloadRecord")) {
				// F10866F4B41CFD1C7B4C043CA51183D7 no-song {"2016-12-08
				// 11:03:32":"2016-12-08 11:03:32\t123.157.178.72\t268249267"}
				int idx = line.indexOf(TAB);
				if (idx > 0 && idx < line.length()) {
					context.write(new Text(line.substring(0, idx)), new Text("DOWNLOAD###" + line.substring(idx + 1)));
				}
			} else if (pathName.contains("ParseUGCLog")) {
				// 1043885510 null null {"20161130
				// 17:40:22":"dan\t113.200.107.20\t20161130
				// 17:40:22\t223756687\t23577555"}
				int idx = line.indexOf(TAB);
				if (idx > 0 && idx < line.length()) {
					context.write(new Text(line.substring(0, idx)), new Text("UGCLOG###" + line.substring(idx + 1)));
				}
			} else if (pathName.contains("AlbumOrder")) {
				// 1280117286 {"20161107 10:46:51":"list|让我留在你身边"}
				int idx = line.indexOf(TAB);
				if (idx > 0 && idx < line.length()) {
					context.write(new Text(line.substring(0, idx)), new Text("ALBUM###" + line.substring(idx + 1)));
				}
			}

		}
	}

	private static class MergeReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String visit = "no-visit" + TAB + "no-search" + TAB + "no-sug";
			String fm = "no-favor" + TAB + "no-del";
			String download = "no-song" + TAB + "no-album";
			String ugc = "no-favor" + TAB + "no-comment" + TAB + "no-gedan";
			String album = "no-order";
			for (Text val : values) {
				String sVal = val.toString();
				if (sVal.startsWith("VISIT###")) {
					visit = sVal.split("###")[1];
				} else if (sVal.startsWith("FM###")) {
					fm = sVal.split("###")[1];
				} else if (sVal.startsWith("DOWNLOAD###")) {
					download = sVal.split("###")[1];
				} else if (sVal.startsWith("UGCLOG###")) {
					ugc = sVal.split("###")[1];
				} else if (sVal.startsWith("ALBUM###")) {
					album = sVal.split("###")[1];
				}
			}
			context.write(key, new Text(visit + TAB + fm + TAB + download + TAB + ugc + TAB + album));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(MergeFMDownoadVisitUGC.class);
		job.setJobName("Evan_MergeFMDownoadVisitUGC");
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
		String out = "/user/work/evan/tmp/MergeFMDownoadVisitUGC";
		Path path = new Path(out);
		hdfs.delete(path, true);

		MergeFMDownoadVisitUGC.runLoadMapReducue(conf, args[0], new Path(out));
	}
}