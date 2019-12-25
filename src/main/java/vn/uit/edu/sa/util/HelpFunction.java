package vn.uit.edu.sa.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;

public class HelpFunction {
	
	public static String getDayOfWeek(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		switch (cal.get(Calendar.DAY_OF_WEEK)) {
			case Calendar.SUNDAY: return "SUN";
			case Calendar.MONDAY: return "MON";
			case Calendar.TUESDAY: return "TUE";
			case Calendar.WEDNESDAY: return "WED";
			case Calendar.THURSDAY: return "THU";
			case Calendar.FRIDAY: return "FRI";
			case Calendar.SATURDAY: return "SAT";
		}
		return "MONDAY";
	}
	
	public static String getMonth(int month) {
		if (month == 0) return "Jan";
		if (month == 1) return "Feb";
		if (month == 2) return "Mar";
		if (month == 3) return "Apr";
		if (month == 4) return "May";
		if (month == 5) return "Jun";
		if (month == 6) return "Jul";
		if (month == 7) return "Aug";
		if (month == 8) return "Sep";
		if (month == 9) return "Oct";
		if (month == 10) return "Nov";
		return "Dec";
	}
		
	public static List<String> getUniversityFanpageIdList(){
		try {
			return Files.readAllLines(Paths.get(System.getProperty("user.dir") + "/" + ConfigReader.readConfig("dir.list.university.fanpage")));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static List<String> getUniversityGroupIdList(){
		try {
			return Files.readAllLines(Paths.get(System.getProperty("user.dir") + "/" + ConfigReader.readConfig("dir.list.university.group")));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
}