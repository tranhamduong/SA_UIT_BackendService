package vn.uit.edu.sa.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {
	
	public static InputStream input;
	private static Properties prop = new Properties();
	
	public ConfigReader() {
		
	}
	
	public static String readConfig(String str) {
		try {
			input = new FileInputStream(System.getProperty("user.dir") + "/config.properties");
			prop.load(input);

			return prop.getProperty(str);
			
		}catch (IOException e) {
			e.printStackTrace();
			System.out.println(e);
		}
		return "error";
	}
}
