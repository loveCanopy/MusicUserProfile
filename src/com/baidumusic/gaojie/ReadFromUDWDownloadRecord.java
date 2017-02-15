package com.baidumusic.gaojie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;

import com.alibaba.fastjson.JSONObject;
import com.baidumusic.evan.Tools;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

public class ReadFromUDWDownloadRecord {
	private static final String TAB = "\t";
	private static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, Text> {
		protected void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			Text txt = new Text();
			BytesRefWritable bid = value.get(1);
			txt.set(bid.getData(), bid.getStart(), bid.getLength());
			String baiduid = txt.toString();

			BytesRefWritable uid = value.get(2);
			txt.set(uid.getData(), uid.getStart(), uid.getLength());
			String userid = String.valueOf(BdussSimpleDecoder64.decode64(txt.toString()));

			if (baiduid.matches("^[0-9a-zA-Z]+$") || userid.matches("^[0-9]{2,}$")) {

				BytesRefWritable ip = value.get(10);
				txt.set(ip.getData(), ip.getStart(), ip.getLength());
				String str_ip = txt.toString();

				BytesRefWritable time = value.get(20);
				txt.set(time.getData(), time.getStart(), time.getLength());
				String str_time = txt.toString();

				BytesRefWritable request = value.get(21);
				txt.set(request.getData(), request.getStart(), request.getLength());
				JSONObject json = Tools.splitMap(txt.toString());
				if (json.isEmpty())
					return;

				String type = json.containsKey("type") ? (String) json.get("type") : "";
				String uniq_id = userid.matches("^[0-9]{2,}$") ? userid : baiduid;
				if (type.equals("down_album")) {
					String album_id = json.containsKey("album_id") ? (String) json.get("album_id") : "0";
					context.write(new Text(uniq_id), new Text(type + TAB + str_time + TAB + str_ip + TAB + album_id));
				} else if (type.equals("downmusic")) {
					String songid = json.containsKey("song_id") ? (String) json.get("song_id") : "0";
					context.write(new Text(uniq_id), new Text(type + TAB + str_time + TAB + str_ip + TAB + songid));
				} /*else if (type.matches("down_mutli")) {
					String uniq_id = userid.matches("^[0-9]{2,}$") ? userid : baiduid;
					String song_num = json.containsKey("song_num") ? (String) json.get("song_num") : "0";
					context.write(new Text(uniq_id), new Text(type + TAB + str_time + TAB + str_ip + TAB + song_num));
				}*/
			}
		}
	}

	private static class RcFileReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			JSONObject json_song = new JSONObject();
			JSONObject json_album = new JSONObject();
			//JSONObject json_mutli = new JSONObject();
			try {
				for (Text txt : values) {
					String str_temp = txt.toString();
					String[] arr_temp = str_temp.split(TAB);
					if (arr_temp.length > 1) {
						if (arr_temp[0].equals("downmusic")) {
							json_song.put(arr_temp[1], str_temp.substring(10));
						} else if (arr_temp[0].equals("down_album")) {
							json_album.put(arr_temp[1], str_temp.substring(11));
						}/* else if (arr_temp[0].equals("down_mutli")) {
							json_mutli.put(arr_temp[1], str_temp.substring(11));
						}*/
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			String str_output = "";
			if (json_song.isEmpty()) {
				str_output = "no-song";
			} else {
				str_output = json_song.toString();
			}
			if (json_album.isEmpty()) {
				str_output += TAB + "no-album";
			} else {
				str_output += TAB + json_album.toString();
			}
//			if (json_mutli.isEmpty()) {
//				str_output += TAB + "no-mutli";
//			} else {
//				str_output += TAB + json_mutli.toString();
//			}
			context.write(key, new Text(str_output));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, Path input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		conf.set("hive.io.file.read.all.columns", "false");
		conf.set("hive.io.file.readcolumn.ids", "1,2,10,20,21"); // baiduid
																	// bduss ip
																	// time url
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReadFromUDWDownloadRecord.class);
		job.setJobName("ReadFromUDWDownloadRecord");
		job.setNumReduceTasks(10);
		job.setMapperClass(RcFileMapper.class);
		job.setReducerClass(RcFileReduce.class);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		RCFileMapReduceInputFormat.addInputPath(job, input);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);
		if (args.length == 0) {
			System.err.println("Usage: class <in>");
			System.exit(1);
		}
		FileSystem hdfs = FileSystem.get(conf);
		String out = "/user/work/xing/ReadFromUDWDownloadRecord";
		Path path = new Path(out);
		hdfs.delete(path, true);

		ReadFromUDWDownloadRecord.runLoadMapReducue(conf, new Path(args[0]), new Path(out));
	}
}