package vn.uit.edu.sa.connectDB;

import org.apache.spark.api.java.JavaSparkContext;

public class ConnectMongoDB {
	
	public ConnectMongoDB() {
		
	}
	
	public static Boolean closeConnection(JavaSparkContext jSparkContext) {
		try {
			jSparkContext.close();
			return true;
		}catch (Exception e) {
			System.out.println(e);
			return false;
		}
	}
}
