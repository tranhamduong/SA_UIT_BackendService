package vn.uit.edu.sa.util;

import java.io.File;
import java.sql.Date;
import java.util.Calendar;
import java.util.Optional;

public class HelpFunction {
	
	public static Optional<String> getExtensionByStringHandling(String filename) {
	    return Optional.ofNullable(filename)
	      .filter(f -> f.contains("."))
	      .map(f -> f.substring(filename.lastIndexOf(".") + 1));
	}
	
	public static void removeUnusedFile() {
		File folder = new File(System.getProperty("user.dir") + ConfigReader.readConfig("dir.pre.finish"));
		File[] listOfFiles = folder.listFiles();
		for(File file : listOfFiles) {
			if (HelpFunction.getExtensionByStringHandling(file.getName()).equals(".crc")) {
				file.delete();
			}else if (file.getName() == "SUCCESS") {
				file.delete();
			}else if (file.length() == 0) {
				file.delete();
			}else if (file.isHidden()) {
				file.delete();
			}
		}
	}
	
	public static void removeUnusedFile(String fileName) {
		File folder = new File(System.getProperty("user.dir") + fileName);
		File[] listOfFiles = folder.listFiles();
		for(File file : listOfFiles) {
			if (HelpFunction.getExtensionByStringHandling(file.getName()).equals(".crc")) {
				file.delete();
			}else if (file.getName() == "SUCCESS") {
				file.delete();
			}else if (file.length() == 0) {
				file.delete();
			}else if (file.isHidden()) {
				file.delete();
			}
		}
	}
	
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
}
