package vn.uit.edu.sa.backEndService;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;

import vn.uit.edu.sa.connectDB.MongoSparkHelper;
import vn.uit.edu.sa.dto.DataFrameToRDDConvertor;
import vn.uit.edu.sa.dto.PostDTO;
import vn.uit.edu.sa.languagePreprocessor.LanguagePreprocessor;
import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.RDDutils;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
	    SparkConfigure sparkConfig = new SparkConfigure();
	   
    	MongoSparkHelper remoteMongoHelper = new MongoSparkHelper(sparkConfig, true, "post");
    	//MongoSparkHelper localMongoHelper = new MongoSparkHelper(sparkConfig, false);
    	
    	DataFrame df = remoteMongoHelper.read("post");
    	df.show();
    	//comment
    	//createdDate:04
    	//message:05
    	//postId: 07
    	
    	//post: postedByUserId
    	//post: _ids
    	
    	//Run on the first time
    	//JavaRDD<PostDTO> allPosts =  DataFrameToRDDConvertor.convertFromDataFrame(df);
    	
    	//allPosts.filter(new Function<PostDTO, Boolean>() {
			
//			@Override
//			public Boolean call(PostDTO v1) throws Exception {
//				
//				return null;
//			}
//		});
    	
      	//MonthCalculating monthCalculating = new MonthCalculating();
    	//monthCalculating.doSentimentAnalysFollowMonth(sparkConfig, allPosts);
    	//monthCalculating.showRDD();
    	
    	//Run on update Data
//    	JavaRDD<PostDTO> onlyNewPosts = DataFrameToRDDConvertor.convertFromDataFrame(df);
//    	
//    	DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
//    	java.sql.Date sqlDate = null;
//		try {
//			sqlDate = new java.sql.Date(dateFormat.parse("06-04-2019").getTime());
//
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		
//    	onlyNewPosts = RDDutils.getOnlyNewPostFromDate(onlyNewPosts, sqlDate);
//    	
//    	LanguagePreprocessor preprocessor = new LanguagePreprocessor(sparkConfig);
//    	preprocessor.save(preprocessor.run(onlyNewPosts));
//    	preprocessor.runVectorize();
    }
}
