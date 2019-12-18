package vn.uit.edu.sa.languagePreprocessor;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.ConfigReader;

public class Standardize implements java.io.Serializable {
	
	private static DataFrame socialWordSet;
	private static DataFrame incorrectWords;
	private static DataFrame correctWords;
	private static Row[] correctRows;
	private static Row[] incorrectRows;
	
	public Standardize(SparkConfigure spark) {
		socialWordSet = getSocialLanguageDictionary(spark);
		
		correctWords = socialWordSet.select("correct");
		incorrectWords = socialWordSet.select("incorrect");
		
		correctRows = correctWords.collect();
		incorrectRows = incorrectWords.collect();
	}
	
	
	public String standardizeSentence(SparkConfigure spark, String string) {
		
		String result = "";
		String[] words = string.split(" ");
		
		for(String word : words) {
			if(word.isEmpty()) continue;
			int count = isSocialLanguage(word);
			if (word == "\n") {
				result += "\n";
			}
			else if (count > 0) {
				//remove teen code, incorrect words, ...				
				result += (correctRows[count-1].toString().substring(1, correctRows[count-1].toString().length() - 1)+" ").toLowerCase();
				continue;
			}
			result += (word.toLowerCase()+" ");
		}
		return result;
	}
	
	private int isSocialLanguage(String word) {
		int count = 0;
		for (Row row : incorrectRows) {
			count += 1;
			if(row.toString().substring(1, row.toString().length()-1).equals(word.toLowerCase())) 
				return count;
		}
		return 0;
	}	
	
	public DataFrame getSocialLanguageDictionary(SparkConfigure spark) {
		String socialLanguageDictionaryName = System.getProperty("user.dir") + ConfigReader.readConfig("dic.pre.social");
		
		SQLContext sqlContext = new SQLContext(spark.getSparkContext());
		
		DataFrame dictionary  = sqlContext.read().json(socialLanguageDictionaryName);
	
		return dictionary;	
	}
}
