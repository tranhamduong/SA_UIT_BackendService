package vn.uit.edu.sa.languagePreprocessor;

import org.apache.spark.api.java.JavaRDD;

import vn.uit.edu.sa.spark.SparkConfigure;
import vn.vitk.tok.Tokenizer;

public class Segmentation implements java.io.Serializable{
	
	public Segmentation() {
		
	}
	
	public JavaRDD<String> wordSegmentation(SparkConfigure spark, JavaRDD<String> inputRDD) {
		String dataFolder = "/export/dat/tok";
		String master = spark.getSparkConf().get("spark.master");	
		
		Tokenizer tokenizer = null;
		tokenizer = new Tokenizer(master, dataFolder + "/lexicon.xml", dataFolder + "/regexp.txt", dataFolder + "/syllables2M.arpa");
		
		return tokenizer.tokenize(inputRDD);
    }
}
