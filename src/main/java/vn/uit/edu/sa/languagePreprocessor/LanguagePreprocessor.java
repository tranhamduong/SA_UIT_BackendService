package vn.uit.edu.sa.languagePreprocessor;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import vn.uit.edu.sa.dto.PostDTO;
import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.ConfigReader;
import vn.uit.edu.sa.util.HelpFunction;
import vn.uit.edu.sa.util.RDDutils;
import org.apache.spark.api.java.function.Function;

public class LanguagePreprocessor implements java.io.Serializable{

	private Standardize standardize = null;
	private Segmentation segmentation = null;
	private RemoveStopWords removeStopWords = null;
	private SparkConfigure spark = null;

	
	public LanguagePreprocessor(SparkConfigure spark) {
		clean();
		this.spark = spark;
		initialize();
	}
	
	public LanguagePreprocessor(SparkConfigure spark, String typeDetail) {
		this.spark = spark;
		initialize();
	}
	
	public static void cleanDir() {
		clean();
	}
	
	private static void clean() {
		File folder = new File(System.getProperty("user.dir") + ConfigReader.readConfig("dir.pre.finish"));
		try {
			FileUtils.deleteDirectory(folder);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	


	private void initialize() {
		standardize = new Standardize(spark);
		segmentation = new Segmentation();
		removeStopWords = new RemoveStopWords();	
	}
	
	public JavaRDD<String> run(JavaRDD<PostDTO> posts) {
		
		JavaRDD<String> rdd = RDDutils.convertFromPostDTOtoString(posts);
		
		rdd = rdd.map(new Function<String, String>() {

			@Override
			public String call(String v1) throws Exception {
				return standardize.standardizeSentence(spark, v1);
			}
		});
		
		rdd = segmentation.wordSegmentation(spark, rdd);
				
		try {
			rdd = removeStopWords.correctData(spark, rdd);
			rdd = RDDutils.removeEmptyRow(rdd);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		RDDutils.showStringRDD(rdd);
		
		return rdd;
	}
	
	public JavaRDD<String>  run(JavaRDD<PostDTO> posts, String typeDetail) {
		JavaRDD<String> result = run(posts);
		return result;
	}
	
	public void save(JavaRDD<String> rdd) {
		rdd.saveAsTextFile(System.getProperty("user.dir") + ConfigReader.readConfig("dir.pre.finish"));
		HelpFunction.removeUnusedFile();
	}
	
	public void save(JavaRDD<String> rdd, String typeDetail) {
		rdd.saveAsTextFile(System.getProperty("user.dir") + ConfigReader.readConfig("dir.pre.finish") + "/" + typeDetail);
		HelpFunction.removeUnusedFile();
		for(int i = 0; i <= 11; i++) {
			HelpFunction.removeUnusedFile(ConfigReader.readConfig("dir.pre.finish") + "/" + typeDetail);
		}
	}
	
	public void runVectorize() {
//		vectorizeFactory = new Vectorize(System.getProperty("user.dir") + "/experiment/191115/not-removeStopWords-result/data/file-to-vectorize");	    
//
//	    vectorizeFactory.run();
	}
}
