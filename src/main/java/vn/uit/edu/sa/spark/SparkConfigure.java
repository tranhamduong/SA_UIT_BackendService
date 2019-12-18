package vn.uit.edu.sa.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import vn.uit.edu.sa.connectDB.MongoSparkHelper;
import vn.vitk.util.SparkContextFactory;

public class SparkConfigure  implements java.io.Serializable{
	private static SparkConf sparkConf = new SparkConf().setMaster("local")
										.set("spark.driver.allowMultipleContexts", "true")
										.setAppName("SA-UIT").set("spark.mongodb.input.uri", MongoSparkHelper.getLocalURI())
										.set("spark.mongodb.output.uri", MongoSparkHelper.getLocalURI());
	private static JavaSparkContext sparkContext = null;
	
	public JavaSparkContext getSparkContext() {
//		if (this.sparkContext == null) {
//			synchronized (SparkContextFactory.class) {
//				if (this.sparkContext == null) {
//					this.sparkContext = new JavaSparkContext(this.sparkConf);
//				}
//			}
//		}
		this.sparkContext = SparkContextFactory.create();
		
		return this.sparkContext;
	}
	
	public JavaSparkContext getRemoteSparkContext() {
//		if (this.sparkContext == null) {
//			synchronized (SparkContextFactory.class) {
//				if (this.sparkContext == null) {
//					this.sparkContext = new JavaSparkContext(this.sparkConf);
//				}
//			}
//		}
		this.sparkContext = SparkContextFactory.createRemoteConnection();
		
		return this.sparkContext;
	}
	
	public void closeJavaSparkContext() {
		this.sparkContext.close();
	}
	
	public SparkConf getSparkConf() {
		return this.sparkConf;
	}
	
}
