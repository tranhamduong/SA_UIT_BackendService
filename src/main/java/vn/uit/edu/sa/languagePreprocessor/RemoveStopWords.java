package vn.uit.edu.sa.languagePreprocessor;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import vn.uit.edu.sa.dto.DTO;
import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.ConfigReader;

public class RemoveStopWords implements java.io.Serializable{
	
	private static Set<String> stopWordSet;
	private static SparkConfigure spark;

	public RemoveStopWords() {
		
	}
	
	/*
	 * 
	 * remove stop word
	 * 
	 */
		public JavaRDD<String>  correctData(SparkConfigure spark, JavaRDD<String> rdd) throws IOException {
			this.spark = spark;
			
			stopWordSet = createListFromDictionary(spark);

			rdd = rdd.map(new Function<String, String>() {

				@Override
				public String call(String v1) throws Exception {
					String temp = removeStopWords(v1);
					return temp;
				}
			});
			return rdd;
		}
		
		public JavaRDD<DTO>  correctDataDTO(SparkConfigure spark, JavaRDD<DTO> rdd) throws IOException {
			this.spark = spark;
			
			stopWordSet = createListFromDictionary(spark);

			rdd = rdd.map(new Function<DTO, DTO>() {

				@Override
				public DTO call(DTO dto) throws Exception {
					dto.setMessage(removeStopWords(dto.getMessage()));
					return dto;
				}
			});
			return rdd;
		}
		
		private String removeStopWords(String string) {
			String result = "";
			String[] words = string.split(" ");
			
			  for(String word : words) { 
				  if(word.isEmpty()) continue;
				  //if (word.equals("\n")) {}
				  if (word.contains("http")) continue;
				  if (word.contains("---")) continue;
				  if (word.contains("___")) continue;
				  if (word.contains("===")) continue;
				  if (word.contains("www")) continue;
				  if (word.contains("...")) continue;
				  if (word.contains(">>")) continue;
				  if (word.contains("<<")) continue;
				  if (word.contains(".com")) continue;
				  if (word.contains(".vn")) continue;
				  if (word.length() >=3) {
					  if (word.contains("?") || word.contains("\"") || word.contains(".") || word.contains(",") || word.contains("/") )
							  continue;
				  }


				  if(this.isStopWord(word)) continue; //remove stopwords 
				 
				  result = result + (word+" "); 
			  }
			return result;
		}
		
		// check if word was a stop word
		private boolean isStopWord(String word) {
			if(word.length() < 2)
				return true;
			if(word.charAt(0) >= '0' && word.charAt(0) <= '9')
				return true;
			if(stopWordSet.contains(word)) {		
				return true;
			}
			return false;
		}
		
		@SuppressWarnings("rawtypes")
		private Set pushDataFromFileToSet(JavaRDD<String> inputFile) throws IOException {
			Set<String> set = new HashSet<String>();
			for(String line:inputFile.collect()){
				set.add(line);
			} 
			return set;
		}
		
		// read each line in file and push words into a set
		@SuppressWarnings("unchecked")
		private Set<String> createListFromDictionary(SparkConfigure spark) throws IOException {
			Set<String> bufferSet = new HashSet<String>();
			JavaRDD<String> inputFile = getDictionaryFile();
			
			bufferSet = this.pushDataFromFileToSet(inputFile);
			
			return bufferSet;
		}
		
//		get stop words dictionary
		public JavaRDD<String> getDictionaryFile() {
			String stopWordsDictionaryFileName = System.getProperty("user.dir") + ConfigReader.readConfig("dic.pre.stopwords");

			JavaRDD<String> dictionaryFile = spark.getSparkContext().textFile(stopWordsDictionaryFileName);
			return dictionaryFile;
		}
}
