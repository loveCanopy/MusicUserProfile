package com.baidumusic.evan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;
import com.alibaba.fastjson.JSONObject;
import com.baidu.passport.hadoop.util.hive.BdussSimpleDecoderUDF;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FileSystem;

public class UserPlayRecord {
	private static final String TAB = "\t";
	private static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, Text> {
		private static String getValue(BytesRefArrayWritable value, int index) {
			Text txt = new Text();
			BytesRefWritable val = value.get(index);
			try {
				txt.set(val.getData(), val.getStart(), val.getLength());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return txt.toString();
		}

		private static String getName(String line, String key) {
			String val = "";
			int idx = line.indexOf(key);
			if (idx >= 0) {
				int idx_end = line.substring(idx).indexOf("/");
				if (idx_end > 0) {
					val = line.substring(key.length() + idx + 1, idx + idx_end);
				} else {
					val = line.substring(key.length() + idx + 1);
				}
			}
			return val;
		}

		protected void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			String baiduid = getValue(value, 1);
			if (baiduid == null || !baiduid.matches("[0-9a-zA-Z]+") || baiduid.length() != 32)
				return;

			String event_bduss = getValue(value, 2);
			BdussSimpleDecoderUDF e = new BdussSimpleDecoderUDF();
			String event_userid = event_bduss.matches("[0-9]+") ? event_bduss : String.valueOf(e.evaluate(event_bduss));
			String logDate = Tools.formatDate2(getValue(value, 20), true);
			if (logDate == null || logDate.length() == 0) {
				logDate = "2048-00-00 00:00:00";
			}

			InputSplit inputSplit = (InputSplit) context.getInputSplit();
			String pathName = ((FileSplit) inputSplit).getPath().toString();
			String event_action = getName(pathName, "event_action");
			String device_type = "other";
			if (event_action.toLowerCase().matches("music_pc_web_play|music_pc_web_other")) {
				device_type = "PCweb";
			} else if (event_action.toLowerCase().matches("music_pc_client_play|music_pc_client_other")) {
				device_type = "PCclient";
			} else if (event_action.toLowerCase().matches("music_wap_common_play|music_wap_common_other")) {
				device_type = "WebApp";
			} else if (event_action.toLowerCase().matches("music_mobile_app_play|music_mobile_app_other")) {
				device_type = "Mobile";
			}
			if (device_type.equals("other"))
				return;

			String event_ip = getValue(value, 10);
			JSONObject json = Tools.splitMap(getValue(value, 21));
			if (json.isEmpty())
				return;

			String ref = json.containsKey("ref") ? (String) json.get("ref") : "";
			String source = json.containsKey("source") ? (String) json.get("source") : "";
			String pid = json.containsKey("pid") ? (String) json.get("pid") : "";
			String rate = json.containsKey("rate") ? (String) json.get("rate") : "000";

			String[] songs = { "suid", "mvid", "sid", "song_id", "songid", "songId" };
			String type = json.containsKey("type") ? (String) json.get("type") : "";
			if (!type.matches("click_favor|click_singleFavor|60play|playend"))
				return;

			String favor = type.matches("click_favor|click_singleFavor") ? "1" : "0";
			String song = "000000000";
			for (String so : songs) {
				if (json.containsKey(so)) {
					song = so;
				}
			}
			String song_id = song.equals("000000000") ? "000000000" : (String) json.get(song);
			String play_time = json.containsKey("position") ? ((String) json.get("position"))
					: (json.containsKey("pt") ? ((String) json.get("pt")) : "0");

			if (device_type.equals("WebApp")) {
				String webapp_device_type = "other";
				if ((pid.equals("304") && ref.equals("radio") && source.equals("wise")) || pid.equals("323")) {
					String event_url = getValue(value, 21);
					String event_useragent = getValue(value, 24);
					if (event_useragent.toLowerCase().matches(".*iphone.*")) {
						webapp_device_type = "iOS";
					} else if (event_useragent.toLowerCase().matches(".*.android.*|.*adr.*")) {
						webapp_device_type = "Android";
					} else if (event_url.matches(".*\\/cms\\/wap.gif.*")) {
						webapp_device_type = "Wap";
					} else if (event_useragent.toLowerCase().matches(".*ipad.*")) {
						webapp_device_type = "iPad";
					} else {
						String os = json.containsKey("os") ? (String) json.get("os") : "other";
						if (os.toLowerCase().matches("android")) {
							webapp_device_type = "Android";
						} else if (os.toLowerCase().matches("iphone")) {
							webapp_device_type = "iOS";
						} else if (os.toLowerCase().matches("ipad")) {
							webapp_device_type = "iPad";
						} else if (os.toLowerCase().matches("wap")) {
							webapp_device_type = "Wap";
						} else {
							webapp_device_type = os;
						}
					}
				}
				device_type = device_type + "_" + webapp_device_type;
			} else if (device_type.equals("Mobile")) {
				String app_device_type = "other";
				String mod = json.containsKey("mod") ? (String) json.get("mod") : "";
				if (mod.matches("bubugao|suoai")) {
					app_device_type = "Android";
				} else if (mod.toLowerCase().matches("android")) {
					app_device_type = "Android";
				} else if (mod.toLowerCase().matches("ios")) {
					app_device_type = "iOS";
				} else if (mod.toLowerCase().matches("ipad")) {
					app_device_type = "iPad";
				}
				if (app_device_type.matches("iOS|iPad")) {
					baiduid = json.containsKey("cuid") ? (String) json.get("cuid")
							: (json.containsKey("uid") ? (String) json.get("uid") : baiduid);
				}
				device_type = device_type + "_" + app_device_type;
				String music_actiontype = getValue(value, 26);
				String music_producttype = getValue(value, 32);
				boolean valid = false;
				if (music_producttype.equals("native") && music_actiontype.equals("mvplay")) {
					valid = true;
				} else if (music_producttype.matches("native|qianqian")) {
					if (music_actiontype.equals("play")) {
						valid = true;
					} else if (music_actiontype.equals("start")) {
						String lp = json.containsKey("lp") ? (String) json.get("lp") : "0";
						if (lp.matches("[0-9]+") && Long.valueOf(lp) > 0 && Long.valueOf(lp) < 1000000) {
							valid = true;
						}
					}
				}
				if (valid) {
					play_time = json.containsKey("pt") ? (String) json.get("pt") : "0";
					event_userid = json.containsKey("luid") ? (String) json.get("luid") : "0";
				}
			} else if (device_type.equals("PCclient")) {
				String bitrate = json.containsKey("bitrate") ? ((String) json.get("bitrate")) : "";
				rate = bitrate.length() >= 3 ? bitrate.substring(0, 3) : "000";
				String tasktype = json.containsKey("tasktype") ? (String) json.get("tasktype") : "999";
				String signtype = json.containsKey("signtype") ? (String) json.get("signtype") : "999";
				play_time = json.containsKey("songplaytime") ? (String) json.get("songplaytime")
						: (json.containsKey("SongPlayTime") ? (String) json.get("SongPlayTime") : "0");
				String downtype = json.containsKey("downtype") ? (String) json.get("downtype") : "999";
				boolean is_play = false;
				if (pid.equals("303") && type.equals("14")) {
					if (Integer.toBinaryString(Integer.valueOf(tasktype)).endsWith("1")
							|| (tasktype.equals("0") && signtype.equals("3"))
							|| (tasktype.equals("0") && signtype.equals("2") && play_time.compareTo("0") > 0)) {
						is_play = true;
					}
				} else if (pid.matches("337|303|313") && type.equals("18") && ref.equals("musicwindow")) {
					is_play = true;
				} else if (pid.matches("337|313") && type.equals("14") && downtype.equals("1")) {
					is_play = true;
				}
				if (is_play) {
					event_userid = json.containsKey("userid") ? (String) json.get("userid")
							: (json.containsKey("uid") ? (String) json.get("uid") : event_userid);
					event_userid = event_userid.equals(String.valueOf(Integer.MAX_VALUE)) ? "0" : event_userid;
				}
			}
			String user_uniq_id = (event_userid.matches("[0-9]+") && !event_userid.equals("0")) ? event_userid : baiduid;
			if (!song_id.equals("000000000")) {
				context.write(
						new Text(device_type + TAB + user_uniq_id + TAB + song_id),
						new Text(logDate + TAB + event_ip + TAB + play_time + TAB + favor + TAB + ref + TAB + rate));
			}
		}
	}

	private static class RcFileReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			JSONObject json = new JSONObject();
			for (Text val : values) {
				String[] vparts = val.toString().split(TAB);
				json.put(vparts[0], val.toString());
			}
			context.write(key, new Text(json.toString()));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		conf.set("hive.io.file.read.all.columns", "false");
		conf.set("hive.io.file.readcolumn.ids", "1,2,8,10,20,21,24,32");
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserPlayRecord.class);
		job.setJobName("Evan_UserPlayRecord");
		job.setNumReduceTasks(100);
		job.setMapperClass(RcFileMapper.class);
		job.setReducerClass(RcFileReduce.class);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		RCFileMapReduceInputFormat.setInputPaths(job, input);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, output);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
	IllegalArgumentException, URISyntaxException {
		Configuration conf = new Configuration();
		// conf.set("mapreduce.job.queuename", "mapred");
		if (args.length < 2) {
			System.err.println("Usage: class <in> <out>");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 2) {
			queue = args[2].matches("hql|dstream|mapred|udw|user|common") ? args[2] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);

		FileSystem hdfs = FileSystem.get(conf);
		// String out = "/user/work/evan/tmp/UserPlayRecord";
		String out = args[1];
		Path path = new Path(out);
		hdfs.delete(path, true);

		UserPlayRecord.runLoadMapReducue(conf, args[0], new Path(out));
	}
}
