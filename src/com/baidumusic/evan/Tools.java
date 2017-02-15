package com.baidumusic.evan;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import com.alibaba.fastjson.JSONObject;
import com.google.common.net.InternetDomainName;

public class Tools {
	public static String COMMA = ",";
	public static String SPACE = " ";
	public static final String PLACEHOLDER = "0";
	private static final DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static Calendar cal = Calendar.getInstance();	
	public static HashMap<String, Integer> itemSet = new HashMap<String, Integer>();
	
	/*
	A+、购买门票	[40]
	A、购买专辑		[35]
	A-、收藏歌手	[30]
	B、搜索结果点击	[25]
	C、sug检索	[20]
	D、搜索后端		[15]
	E、发表评论		[10]
	F、下载歌曲		[10]
	G、添加到歌单	[10]
	G-、分享歌曲	[10]
	H、收藏专辑		[10]
	H-、分享专辑	[10]
	I、收藏歌单		[5]
	J、电台-喜欢	[5]
	J、电台-删除	[5]
	K、点赞		[5]
	L、浏览评论列表	[5]
	L、播放日志		[5]
	M、访问日志		[5]
	 */
	static {
		itemSet.put("gmmp", 100);
		itemSet.put("gmzj", 80);
		itemSet.put("scgs", 60);
		itemSet.put("ssjgdj", 50);
		itemSet.put("sugjs", 40);
		itemSet.put("sshd", 30);
		itemSet.put("fbpl", 20);
		itemSet.put("xzgq", 20);
		itemSet.put("tjdgd", 20);
		itemSet.put("fxgq", 20);
		itemSet.put("sczj", 20);
		itemSet.put("fxzj", 20);
		itemSet.put("scgd", 10);
		itemSet.put("dtxh", 10);
		itemSet.put("dtsc", -10);
		itemSet.put("dz", 10);
		itemSet.put("llpllb", 10);
		itemSet.put("bfrz", 10);
		itemSet.put("fwrz", 10);
	}

	public static String formatPublishtime(String publishtime) {
		String res = "null";
		publishtime = publishtime.split("-")[0];
		if (!publishtime.matches("[0-9]+") || publishtime.equals("0000")) {
			return res;
		}
		int ipt = Integer.valueOf(publishtime);
		if (ipt < 1950) {
			res = "1900-1949";
		} else if (ipt < 1960) {
			res = "1950-1959";
		} else if (ipt < 1970) {
			res = "1960-1969";
		} else if (ipt < 1980) {
			res = "1970-1979";
		} else if (ipt < 1990) {
			res = "1980-1989";
		} else if (ipt < 2000) {
			res = "1990-1999";
		} else if (ipt < 2010) {
			res = "2000-2009";
		} else if (ipt < 2020) {
			res = "2010-2019";
		} else {
			res = "null";
		}
		
		return res;
	}
	/**
	 * get value of given @fieldName from log @line
	 * 
	 * @param fieldName
	 *            whole name of field such as "&hid="
	 * @param line
	 * @return
	 */
	public static String getValue(String fieldName, String line) {
		String value = "";
		int index = line.indexOf(fieldName);
		if (index > 0) {
			int endIndex = line.indexOf("&", index + fieldName.length());
			if (endIndex > 0) {
				value = line.substring(index + fieldName.length(), endIndex);
			} else {
				endIndex = line.indexOf(" ", index + fieldName.length());
				if (endIndex > 0) {
					value = line.substring(index + fieldName.length(), endIndex);
				}
			}
		}
		return value;
	}

	public static Map<String, String> months = new HashMap<String, String>(12);
	static {
		months.put("Jan", "01");
		months.put("Feb", "02");
		months.put("Mar", "03");
		months.put("Apr", "04");
		months.put("May", "05");
		months.put("Jun", "06");
		months.put("Jul", "07");
		months.put("Aug", "08");
		months.put("Sep", "09");
		months.put("Oct", "10");
		months.put("Nov", "11");
		months.put("Dec", "12");
	}

	/**
	 * format given @time from "[03/Sep/2012:00:00:19" to "YYYYmmdd:HH:MM:SS"
	 * 
	 * @param time
	 *            picked from log which is like "[03/Sep/2012:00:00:19"
	 * @param withHour
	 *            if true, "HH:MM:SS" is returned along with "YYYYmmdd"
	 * @return
	 */
	public static String formatDate(String time, boolean withHour) {
		if (time.matches("[0-9]{2}/[a-zA-Z]+/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2}")
				|| time.matches("\\[([\\w:/]+\\s[+\\-]\\d{4})\\]")) {
			String[] parts = time.split(":", 2);// date and time
			String month = parts[0].split("/", 3)[1];
			String year = parts[0].split("/", 3)[2];
			String day = parts[0].split("/", 3)[0].replace("[", "");
			return (year + "-" + months.get(month) + "-" + day) + (withHour ? " " + parts[1].split(" ")[0] : "");
		}
		return "";
	}
	
	public static String dateToLocal(String date) {
		if (date.matches("\\[([\\w:/]+\\s[+\\-]\\d{4})\\]")) {
			date = date.split("\\s")[0].substring(1);
		}
		String pattern = "dd/MMM/yyyy:HH:mm:ss";
		String tar_pat = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sour_sdf = new SimpleDateFormat(pattern, Locale.US);
		SimpleDateFormat tar_sdf = new SimpleDateFormat(tar_pat, Locale.US);
		try {
			return tar_sdf.format(sour_sdf.parse(date));
		} catch (ParseException localParseException) {
		}
		return "";
	}

	/**
	 * format given @time from "[03/Sep/2012:00:00:19" to "YYYYmmdd"
	 * 
	 * @param time
	 *            picked from log which is like "[03/Sep/2012:00:00:19"
	 * @return
	 */
	public static String formatDate(String time) {
		return formatDate(time, false);
	}

	/**
	 * iterate @set1 to check how many of them exists in @set2
	 * 
	 * @param set1
	 * @param set2
	 * @return
	 */
	public static int getSetIntersection(Set<String> set1, Set<String> set2) {
		int count = 0;
		Iterator<String> it = set1.iterator();
		while (it.hasNext()) {
			if (set2.contains(it.next())) {
				count++;
			}
		}
		return count;
	}
	
	public static byte[] toByteArray(int iSource, int iArrayLen) {
	    byte[] bLocalArr = new byte[iArrayLen];
	    for (int i = 0; (i < 4) && (i < iArrayLen); i++) {
	        bLocalArr[i] = (byte) (iSource >> 8 * i & 0xFF);
	    }
	    return bLocalArr;
	}

	public static long BytetoLong(byte[] bRefArr) {
	    long iOutcome = 0;
	    byte bLoop;

	    for (int i = 0; i < bRefArr.length; i++) {
	        bLoop = bRefArr[i];
	        iOutcome += (bLoop & 0xFF) << (8 * i);
	    }
	    return iOutcome;
	}
	
	public static String filterCol(int col, String todel) {
		String res = "";
		String[] nums = todel.split(",");
		HashSet<Integer> hSet = new HashSet<Integer>();
		for (String n : nums) {
			hSet.add(Integer.valueOf(n));
		}
		for (int i=0; i<col-1; i++) {
			if (!hSet.contains(i)) {
				res += i + ",";
			}
		}
		return res;
	}
	
	public static JSONObject splitMap(String request) {
		JSONObject json = new JSONObject();
		if (request == null || request.length() == 0)
			return json;
		int index = request.indexOf("?");
		if (index > 0 && index < request.length()-1) {
			request = request.substring(index+1);
		}
		String[] logList = request.split("&");
		for (String log : logList) {
			String[] kv = log.split("=");
			if (2 == kv.length) {
				json.put(kv[0], kv[1]);
			}
		}
		return json;
	}
	
	public static String str2stamp(String user_time, String DATE_FORMAT) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        String re_time = null;
        Date d;
        try {
            d = sdf.parse(user_time);
            long l = d.getTime();
            String str = String.valueOf(l);
            re_time = str.substring(0, 10);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return re_time;
    }
	
	public static String stamp2str2(String cc_time) {//2016-02-27 20:56:51
        String re_StrTime = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lcc_time = Long.valueOf(cc_time);
        re_StrTime = sdf.format(new Date(lcc_time * 1000L));
        return re_StrTime;
    }

	public static String formatDate2(String time, boolean withHour) {
		if (time.matches("[0-9]{4}-[a-zA-Z]+-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")) {
			String[] parts = time.split(" ", 2);// date and time
			String year = parts[0].split("-", 3)[0];
			String month = parts[0].split("-", 3)[1];
			String day = parts[0].split("-", 3)[2];
			return (year + "-" + months.get(month) + "-" + day) + (withHour ? " " + parts[1].split(" ")[0] : "");
		}
		return time;
	}
	
	public static String getTopPrivateDomain(final String domain) {
		String result = "";
		try {
			final InternetDomainName from = InternetDomainName.from(domain);
			final InternetDomainName topPrivateDomain = from.topPrivateDomain();
			result = topPrivateDomain.name();
		} catch (Exception e) {
			result = "";
		}
		return result;
	}
	
	public static String formatRate(String rate) {
		String res = "000";
		if (!rate.matches("[0-9]+")) {
			return res;
		}
		int irate = Integer.valueOf(rate);
		if (irate <= 0) {
			return res;
		}
		if (irate < 32 || irate < (32 + 64) / 2) {
			res = "32";
		} else if (irate < 64 || irate < (64 + 96) / 2) {
			res = "64";
		} else if (irate < 96 || irate < (96 + 128) / 2) {
			res = "96";
		} else if (irate < 128 || irate < (128 + 256) / 2) {
			res = "128";
		} else if (irate < 256 || irate < (256 + 320) / 2) {
			res = "256";
		} else if (irate < 320 || irate < (320 + 500) / 2) {
			res = "320";
		} else {
			res = "320plus";
		}

		return res;
	}
	
	public static int parseWeekOfMonth(String date) {
		Date d = new Date();
		try {
			d = dateformat.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cal.setTime(d);
		int weekOfMonth = cal.get(Calendar.WEEK_OF_MONTH);
		return weekOfMonth;
	}
	
	public static String parseWeek(String date) {
		Date d = new Date();
		try {
			d = dateformat.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cal.setTime(d);
		String res = null;
		
		int week = cal.get(Calendar.DAY_OF_WEEK) - 1;
		switch (week) {
		case 0:
			res = "周日";
			break;
		case 1:
			res = "周一";
			break;
		case 2:
			res = "周二";
			break;
		case 3:
			res = "周三";
			break;
		case 4:
			res = "周四";
			break;
		case 5:
			res = "周五";
			break;
		case 6:
			res = "周六";
			break;
		default:
			res = "星期";
			break;
		}
		
		return res;
	}
	
	public static String parseHour(String date) {
		Date d = new Date();
		try {
			d = dateformat.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cal.setTime(d);
		String range = null;
		
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		switch (hour) {
		case 0:
			range = "00:00-01:00";
			break;
		case 1:
			range = "01:00-02:00";
			break;
		case 2:
			range = "02:00-03:00";
			break;
		case 3:
			range = "03:00-04:00";
			break;
		case 4:
			range = "04:00-05:00";
			break;
		case 5:
			range = "05:00-06:00";
			break;
		case 6:
			range = "06:00-07:00";
			break;
		case 7:
			range = "07:00-08:00";
			break;
		case 8:
			range = "08:00-09:00";
			break;
		case 9:
			range = "09:00-10:00";
			break;
		case 10:
			range = "10:00-11:00";
			break;
		case 11:
			range = "11:00-12:00";
			break;
		case 12:
			range = "12:00-13:00";
			break;
		case 13:
			range = "13:00-14:00";
			break;
		case 14:
			range = "14:00-15:00";
			break;
		case 15:
			range = "15:00-16:00";
			break;
		case 16:
			range = "16:00-17:00";
			break;
		case 17:
			range = "17:00-18:00";
			break;
		case 18:
			range = "18:00-19:00";
			break;
		case 19:
			range = "19:00-20:00";
			break;
		case 20:
			range = "20:00-21:00";
			break;
		case 21:
			range = "21:00-22:00";
			break;
		case 22:
			range = "22:00-23:00";
			break;
		case 23:
			range = "23:00-24:00";
			break;
		default:
			range = "00:00-24:00";
			break;
		}
		
		return range;
	}
	
	public static void main(String[] args) throws IOException {
		// [18/Sep/2013:06:49:57 +0000]
		System.out.println(formatDate("18/Sep/2013:06:49:57", true));
		System.out.println(formatDate("[18/Sep/2013:06:49:57 +0000]", true));
		System.out.println(dateToLocal("18/Sep/2013:06:49:57"));
		System.out.println(dateToLocal("[18/Sep/2013:06:49:57 +0000]"));
		System.out.println(stamp2str2("1475077273"));
		System.out.println("there.. " + formatDate2("2016-Sep-01 20:12:17", true));
		System.out.println(getTopPrivateDomain("www.baidu.com"));
		System.out.println(formatPublishtime("0000-00-00"));
		System.out.println(parseHour("2016-12-19 20:12:17"));
		System.out.println(parseWeek("2016-12-19 20:12:17"));
	}
}
