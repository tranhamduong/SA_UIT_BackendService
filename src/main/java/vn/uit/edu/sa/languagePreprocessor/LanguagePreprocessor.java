package vn.uit.edu.sa.languagePreprocessor;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;

import vn.uit.edu.sa.dto.DTO;
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
	
	
	public JavaRDD<DTO> run(JavaRDD<DTO> rdd) {
		
		//JavaRDD<String> rdd = RDDutils.convertFromDTOtoString(posts);
		
		rdd = rdd.map(new Function<DTO, DTO>() {

			@Override
			public DTO call(DTO dto) {
				dto.setMessage(standardize.standardizeSentence(spark, dto.getMessage()));
				return dto;
			}
		});
		
		rdd = segmentation.wordSegmentationDTO(spark, rdd);

		try {
			rdd = removeStopWords.correctDataDTO(spark, rdd);
			rdd = RDDutils.removeEmptyRow(rdd);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return rdd;
	}
	
	public void runVectorize() {
//		vectorizeFactory = new Vectorize(System.getProperty("user.dir") + "/experiment/191115/not-removeStopWords-result/data/file-to-vectorize");	    
//
//	    vectorizeFactory.run();
	}
}