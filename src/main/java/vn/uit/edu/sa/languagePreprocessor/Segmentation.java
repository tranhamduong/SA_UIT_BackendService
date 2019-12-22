package vn.uit.edu.sa.languagePreprocessor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import vn.uit.edu.sa.dto.DTO;
import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.RDDutils;
import vn.vitk.tok.Tokenizer;

public class Segmentation implements java.io.Serializable{
	
	private Tokenizer tokenizer = null;

	
	public Segmentation() {
		
	}
	
	public JavaRDD<String> wordSegmentation(SparkConfigure spark, JavaRDD<String> inputRDD) {
		String dataFolder = "/export/dat/tok";
		String master = spark.getSparkConf().get("spark.master");	
		
		Tokenizer tokenizer = null;
		tokenizer = new Tokenizer(master, dataFolder + "/lexicon.xml", dataFolder + "/regexp.txt", dataFolder + "/syllables2M.arpa");
		
		return tokenizer.tokenize(inputRDD);
    }
	
	public JavaRDD<DTO> wordSegmentationDTO(final SparkConfigure spark, JavaRDD<DTO> inputRDD) {
		String dataFolder = "/export/dat/tok";
		String master = spark.getSparkConf().get("spark.master");	
		
		tokenizer = new Tokenizer(master, dataFolder + "/lexicon.xml", dataFolder + "/regexp.txt", dataFolder + "/syllables2M.arpa");
		JavaRDD<DTO> res = null;
		res = inputRDD.map(new Function<DTO, DTO>() {

			@Override
			public DTO call(DTO dto) throws Exception {
				dto.setMessage(tokenizer.tokenizeOneLine(spark, dto.getMessage()));
				return dto;
			}
		});
		return inputRDD;
    }
}