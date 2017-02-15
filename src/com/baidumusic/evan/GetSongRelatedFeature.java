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

import jodd.util.StringUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;

public class GetSongRelatedFeature {
	private final static String TAB = "\t";
	private final static String HNZ = "|";
	private static class GetTopMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//014FA37C01CB3485E42D2FE521379741	7351648 128,256	0|0|1|0|0|0|0|0
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (lparts.length > 2) {
				context.write(new Text(lparts[1]), new Text("1" + TAB + line.length()));
			}
		}
	}
	
	private static class GetTopCombiner extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			int count = 0;
			long size = 0;
			for (Text t : values) {
				String[] vpart = t.toString().split(TAB);
				count += Integer.valueOf(vpart[0]);
				size += Long.valueOf(vpart[1]);
			}
			context.write(key, new Text(count + TAB + size));
		}
	}

	private static class GetTopReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			int count = 0;
			long size = 0;
			for (Text t : values) {
				String[] vpart = t.toString().split(TAB);
				count += Integer.valueOf(vpart[0]);
				size += Long.valueOf(vpart[1]);
			}
			if (count > 1000 || size > 1e+8) {
				context.write(key, new Text(count + TAB + size));
			}
		}
	}
	
	private static class TopSongMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static HashSet<String> hSet = new HashSet<String>();
		private static HashSet<String> lSet = new HashSet<String>();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			File popsong = new File("popularity.song");
			BufferedReader br = new BufferedReader(new FileReader(popsong));
			String line = "";
			while ((line = br.readLine()) != null) {
				hSet.add(line.split("\\s+")[0]);
			}
			br.close();
			System.out.println("hSet.size() = " + hSet.size());
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (7 == lparts.length && lparts[2] != null && lparts[2].matches("[0-9]+")) {
				if (hSet.contains(lparts[2])) {
					String singerName = lparts[1];
					String songId = lparts[2];
					String laguage = lparts[4];
					String publishTime = lparts[5];
					String style = lparts[6];
					lSet.add(songId);
					context.write(new Text(songId),
							new Text(singerName + HNZ + laguage + HNZ + publishTime + HNZ + style));
				}
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (String song : hSet) {
				if (!lSet.contains(song)) {
					context.write(new Text(song), new Text());
				}
			}
		}
	}

	private static class TopSongReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			String songinfo = "null" + HNZ + "null" + HNZ + "0000-00-00" + HNZ + "null";
			for (Text val : values) {
				if (val.toString() != null && val.toString().length() > 0) {
					songinfo = val.toString();
				}
			}
			context.write(key, new Text(songinfo));
		}
	}
	
	private static class JoinSongMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, String> hMap = new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			File stopwordFile = new File("songs.info");
			BufferedReader br = new BufferedReader(new FileReader(stopwordFile));
			String line = "";
			while ((line = br.readLine()) != null) {
				int idx = line.indexOf(TAB);
				if (idx > 0 && idx < line.length()) {
					hMap.put(line.substring(0, idx), line.substring(idx+1));
				}
			}
			br.close();
			System.out.println("hMap.size() = " + hMap.size());
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			InputSplit inputSplit = (InputSplit)context.getInputSplit();
			String pathName = ((FileSplit)inputSplit).getPath().toString();
			String line = value.toString();
			if (pathName.contains("music_quku")) {
				String[] lparts = line.split(TAB);
				if (7 == lparts.length && lparts[2] != null && lparts[2].matches("[0-9]+")) {
					String singerName = lparts[1];
					String songId = lparts[2];
					String laguage = lparts[4];
					String publishTime = lparts[5];
					String style = lparts[6];
					context.write(new Text(songId),
							new Text("quku$" + singerName + HNZ + laguage + HNZ + publishTime + HNZ + style));
				}
			} else {
				////014FA37C01CB3485E42D2FE521379741	7351648 128,256	0|0|1|0|0|0|0|0
				String song_id = line.split(TAB)[1];
				if (hMap.containsKey(song_id)) {
					String songinfo = hMap.get(song_id);
					context.write(new Text("#Done#" + line + "#Done#" + songinfo), new Text());
				} else {
					context.write(new Text(song_id), new Text(line));
				}
			}
		}
	}

	private static class JoinSongReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			if (key.toString().startsWith("#Done#")) {
				String[] lparts = StringUtil.split(key.toString(), "#Done#");
				if (2 == lparts.length) {
					context.write(new Text(lparts[0]), new Text(lparts[1]));
				}
				return;
			}
			HashSet<String> hSet = new HashSet<String>();
			String songinfo = "null" + HNZ + "null" + HNZ + "0000-00-00" + HNZ + "null";
			for (Text val : values) {
				if (val.toString().startsWith("quku$")) {
					songinfo = val.toString().split("ku\\$")[1];
				} else {
					hSet.add(val.toString());
				}
			}
			if (hSet.size() > 0) {
				for (String line : hSet) {
					context.write(new Text(line), new Text(songinfo));
				}
			}
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input1, String input2) 
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(conf);
		String out1 = "/user/work/evan/output/GetSongRelatedFeature1";
		String out2 = "/user/work/evan/output/GetSongRelatedFeature2";
		String out3 = "/user/work/evan/output/GetSongRelatedFeature3";  
		hdfs.delete(new Path(out1), true);
		hdfs.delete(new Path(out2), true);
		hdfs.delete(new Path(out3), true);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(GetSongRelatedFeature.class);
		job.setJobName("Evan_GetSongRelatedFeature-L1");
		job.setNumReduceTasks(1);
		job.setMapperClass(GetTopMapper.class);
		job.setCombinerClass(GetTopCombiner.class);
		job.setReducerClass(GetTopReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input2);
		FileOutputFormat.setOutputPath(job, new Path(out1));
		job.waitForCompletion(true);
		
		job = Job.getInstance(conf);
		job.setJarByClass(GetSongRelatedFeature.class);
		job.setJobName("Evan_GetSongRelatedFeature-L2");
		job.setNumReduceTasks(1);
		job.setMapperClass(TopSongMapper.class);
		job.setReducerClass(TopSongReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.addCacheFile(new URI(out1+"/part-r-00000#popularity.song"));
		FileInputFormat.setInputPaths(job, input1);
		FileOutputFormat.setOutputPath(job, new Path(out2));
		job.waitForCompletion(true);
		
		job = Job.getInstance(conf);
		job.setJarByClass(GetSongRelatedFeature.class);
		job.setJobName("Evan_GetSongRelatedFeature-L3");
		job.setNumReduceTasks(100);
		job.setMapperClass(JoinSongMapper.class);
		job.setReducerClass(JoinSongReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.addCacheFile(new URI(out2 + "/part-r-00000#songs.info"));
	    
		FileInputFormat.setInputPaths(job, input1 + "," + input2);
		FileOutputFormat.setOutputPath(job, new Path(out3));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, 
			ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		if (args.length == 0) {
			System.err.println("Usage: class <in> <out> ");
			System.exit(1);
		}
		
		String queue = "mapred";
		if (args.length > 2) {
			queue = args[2].matches("hql|dstream|mapred|udw|user|common") ? args[2] : "mapred"; 
		}
		conf.set("mapreduce.job.queuename", queue);
		
		GetSongRelatedFeature.runLoadMapReducue(conf, args[0], args[1]);
	}
}