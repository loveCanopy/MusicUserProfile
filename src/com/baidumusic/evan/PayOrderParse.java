package com.baidumusic.evan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.net.URISyntaxException;

public class PayOrderParse {
	private final static String TAB = "|";
	private static class SongMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String getRefer(JSONObject extend, String desc) {
			String fr = extend.getString("fr");
			if (fr != null) {
				return fr;
			} else {
				String from = extend.getString("from");
				if (from == null) {
					return "";
				} else {
					if (desc.toLowerCase().matches(".*webapp.*") 
							&& from.matches(".*http://.*")) {
						return "webapp";
					} else {
						return from;
					}
				}
			}
		}
		
		private String getDevice(JSONObject extend) {
			String fr = extend.containsKey("fr") ? extend.getString("fr").toLowerCase() : "";
			String from = extend.containsKey("from") ? extend.getString("from").toLowerCase() : "";

			if (fr.matches(".*android.*") || from.matches(".*android.*")) {
				return "android";
			} else if (fr.matches(".*ios.*") || from.matches(".*ios.*")) {
				return "ios";
			} else {
				return "other";
			}
		}
		
		private String getTerminal(JSONObject extend) {
			String fr = extend.containsKey("fr") ? extend.getString("fr").toLowerCase() : "";
			String from = extend.containsKey("from") ? extend.getString("from").toLowerCase() : "";

			if (fr.matches("web") || fr.matches("pay_.*")) {
				return "pcweb";
			} else if (from.matches(".*android.*") || fr.matches(".*android.*")
					|| from.matches(".*ios.*") || fr.matches(".*ios.*")) {
				return "wiseclient";
			} else if (from.matches(".*music_web.*")) {
				return "wiseweb";
			} else {
				return "undefined";
			}
		}
		
		private String getProduct(String product_id) {
			String res = product_id;
			if (product_id.equals("1000010")) {
				res = "king";
			} else if (product_id.equals("1000013")) {
				res = "shoumai";
			} else if (product_id.equals("1000012")) {
				res = "lossless";
			} else if (product_id.equals("1000014")) {
				res = "vip_other";
			} else if (product_id.equals("1000015")) {
				res = "kingbao";
			} else if (product_id.equals("1000016")) {
				res = "liuliang";
			}
			return res;
		}
		
		/*
		-- dataType为add表示新购买 serveTime按月计 表示新购买vip期限
      	-- dataType为upgrade表示升级为白金会员 serveTime按天计 表示升级vip期限
      	-- dataType为up表示续费 serveTime按月计 表示续费vip期限
		*/
		
		private String getValue(String line, String key) {
			String val = "";
			if (line == null || !line.contains(key))
				return val;
			key = "\"" + key + "\"";
			int idx = line.indexOf(key);
			String oth = line.substring(idx+key.length()+1);
			int idx2 = oth.indexOf("\"");
			if (idx2 > 0) {
				String oth3 = oth.substring(idx2+1);
				int idx3 = oth3.indexOf("\"");
				val = oth3.substring(0, idx3);
			}
			return val;
		}
		
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			InputSplit inputSplit = (InputSplit)context.getInputSplit();
			String pathName = ((FileSplit)inputSplit).getPath().toString();
			String line = value.toString().replace("|", "");
			//  /music/package/music_ods_vip_order_info/20160928/music_ods_vip_order_info
			//  /music/package/music_ods_king_order_info/20160928/music_ods_king_order_info
			if (pathName.contains("music_ods_vip_order_info")) {
				String[] lparts = line.split("\001");
				if (16 != lparts.length) return;
				String product = "vip_order";
				//String trans_id = lparts[0];
				String baidu_id = lparts[1];
				if (baidu_id.equals("0")) return;
				//String order_id = lparts[2];
				//String wallet_bill = lparts[3];
				String service_type = lparts[4];
				//String service_level = lparts[5];
				String vip_type = lparts[5];
				String start_time = lparts[6];
				String end_time = lparts[7];
				//String update_time = lparts[8];
				String pay_time = lparts[8];
				String money = lparts[9];
				String num = lparts[10];
				String status = lparts[11];
				//String xrank = lparts[12];
				String desc_info = lparts[13];
				//String wallet_detail = lparts[14];
				String sevice_detail = lparts[15];
				try {
					JSONObject extend = JSON.parseObject(sevice_detail);
					String refer = getRefer(extend, desc_info);
					String action_type = extend.containsKey("dataType") ? extend.getString("dataType") : "";
					String serveTime = extend.containsKey("serveTime") ? extend.getString("serveTime") : "";
					String alertpay = extend.containsKey("alertpay") ? extend.getString("alertpay") : "";
					String device_type = getDevice(extend);
					String event_terminal_type = getTerminal(extend);
					
					context.write(new Text(baidu_id),
							new Text(
							product + TAB + 
							baidu_id + TAB + 
							start_time + TAB +  
							end_time + TAB +  
							pay_time + TAB + 
							money + TAB +  
							refer + TAB + 
							vip_type + TAB + 
							action_type + TAB +  
							status + TAB + 		// 付款状态
							event_terminal_type + TAB + 
							
							device_type + TAB + 
							num + TAB + 
							service_type + TAB + 
							serveTime + TAB + 
							alertpay
							));
				} catch(com.alibaba.fastjson.JSONException e) {
					//TODO nothing..
				} catch(java.lang.ClassCastException e) {
					//TODO nothing..
				}
			} else if (pathName.contains("music_ods_king_order_info")) {
				String[] lparts = line.split("\001");
				if (23 != lparts.length) return;
				//String id = lparts[0];
				String baidu_id = lparts[1];
				if (baidu_id.equals("0")) return;
				String product_id = lparts[2];
				//String order_id = lparts[3];
				String money = lparts[4];
				String list_money = lparts[5]; //mark discount
				//String good_name = lparts[6];
				//String good_url = lparts[7];
				String good_type = lparts[8];
				String prepay_type = lparts[9];
				String pay_type = lparts[10]; //付款类型，1点券收银台 2点券扣款 3百付宝 4支付宝
				String pay_status = lparts[11];
				//String trans = lparts[12];
				String deliver_status = lparts[13];
				String start_time = Tools.stamp2str2(lparts[14]); //订单创建时间
				String pay_time = Tools.stamp2str2(lparts[15]);  //订单支付时间
				String is_succ = pay_time.compareTo(start_time) > 0 ? "1" : "0";
				String deliver_time = Tools.stamp2str2(lparts[16]); //发货完成时间
				String refund_time = lparts[17]; //退款时间
				//String info = lparts[18];
				String source = lparts[19];  //订单来源
				//String pageurl = lparts[20];
				//String bb_song = lparts[21];
				String product = getProduct(product_id);
				String event_terminal_type = "pcweb";
				
				context.write(new Text(baidu_id),
						new Text(
						product + TAB +
						baidu_id + TAB + 
						start_time + TAB + 
						deliver_time + TAB + 
						pay_time + TAB + 
						money + TAB + 
						source + TAB + 
						"king" + TAB +
						prepay_type + TAB + 
						is_succ + TAB + 
						event_terminal_type + TAB +
						
						list_money + TAB + 
						good_type + TAB + 
						pay_type + TAB + 
						pay_status + TAB + 
						deliver_status + TAB + 
						refund_time //+ TAB + info
						));
			}  else if (pathName.contains("music_ods_yyr_order_info")) {
				String[] lparts = line.split("\001");
				if (14 != lparts.length) return;
				
				//String trans_id = lparts[0];
				String baidu_id = lparts[1];
				if (baidu_id.equals("0")) return;
				String artist_id = lparts[2];
				//String order_id = lparts[3];
				String price_deal = lparts[4];
				String price_old = lparts[5];
				String order_type = lparts[6];
				String order_key = lparts[7];
				String start_time = lparts[8];
				String end_time = lparts[9];
				String deal_day = lparts[10];
				String status = lparts[11];
				String through = lparts[12];
				String sevice_detail = lparts[13];
				String alertpay = getValue(sevice_detail, "alertpay");
				String fr = getValue(sevice_detail, "fr");
				String buy_type = getValue(sevice_detail, "buy_type");
				String is_succ = deal_day.compareTo(start_time) > 0 ? "1" : "0";
				String event_terminal_type = "pcweb";
				String vip_type = "";
				
				context.write(new Text(baidu_id),
						new Text(
						"yyr" + TAB +
						baidu_id + TAB + 
						start_time + TAB + 
						end_time + TAB + 
						deal_day + TAB + 
						price_deal + TAB + 
						fr + TAB +
						vip_type + TAB + 
						through + TAB +
						is_succ + TAB +
						event_terminal_type + TAB +
						alertpay + TAB + 
						artist_id + TAB +
						price_old + TAB + 
						order_type + TAB + 
						order_key + TAB + 
						status + TAB + 
						through + TAB + 
						buy_type));
			}
		}
	}

	private static class SongReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			int cnt = 0;
			for (Text val : values) {
				if (cnt++ == 0) {
					sb.append(val.toString());
				} else {
					sb.append(";").append(val.toString());
				}
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(PayOrderParse.class);
		job.setJobName("Evan_PayOrderParse");
		job.setNumReduceTasks(1);
		job.setMapperClass(SongMapper.class);
		job.setReducerClass(SongReduce.class);
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, 
			InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		if (args.length == 0) {
			System.err.println("Usage: rcfile <in>");
			System.exit(1);
		}
		
		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred"; 
		}
		conf.set("mapreduce.job.queuename", queue);
		
		FileSystem hdfs = FileSystem.get(conf);
		String out = "/user/work/evan/tmp/PayOrderParse";
		Path path = new Path(out);
		hdfs.delete(path, true);
		
		PayOrderParse.runLoadMapReducue(conf, args[0], new Path(out));
	}
}