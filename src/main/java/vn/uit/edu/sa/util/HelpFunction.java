package vn.uit.edu.sa.util;

import java.io.File;
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
}
