package vn.uit.edu.sa.backEndService;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;

import vn.uit.edu.sa.connectDB.MongoSparkHelper;
import vn.uit.edu.sa.dto.DataFrameToRDDConvertor;
import vn.uit.edu.sa.dto.DTO;
import vn.uit.edu.sa.spark.SparkConfigure;

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
	    MongoSparkHelper mongod = null;
	    
	    if (args[0].equals("remote")){
	    	mongod = new MongoSparkHelper(sparkConfig, true, "post");
	    }else if (args[0].equals("local")){
	    	mongod = new MongoSparkHelper(sparkConfig, false, "post");
	    }
	    
    	    	
    	DataFrame postDF = mongod.read("post");    	
    	DataFrame commentDF = mongod.read("comment");
    	
    	//comment
    	//createdDate:04
    	//message:05
    	//postId: 07
    	
    	//post: postedByUserId
    	//post: _ids
    	
    	//args[1] = "01-01-2019";
    	
    	//Run on the first time
    	JavaRDD<DTO> postRDD =  DataFrameToRDDConvertor.convertFromDataFrameToPostDTO(postDF);
    	
    	JavaRDD<DTO> commentRDD = DataFrameToRDDConvertor.convertFromDataFrameToCommentDTO(commentDF);
    	
    	DataFilter filter = new DataFilter();
    	
    	JavaRDD<DTO> weekPostRDD = filter.weekPostDTPFilterFactory(postRDD, new String[] {args[1]});
    	JavaRDD<DTO> weekCommentRDD = filter.weekPostDTPFilterFactory(postRDD, new String[] {args[1]});

    	
    	postRDD = filter.postDTOFilterFactory(postRDD, new String[] {args[1]});
    	commentRDD = filter.commentDTOFilterFactory(commentRDD, new String[] {args[1]});
    	
      	StatisticCalculator statisticCalculator = new StatisticCalculator();
      	statisticCalculator.doSentimentAnalyst(sparkConfig, postRDD, "MONTH", "POST");
      	statisticCalculator.doSentimentAnalyst(sparkConfig, commentRDD, "MONTH", "COMMENT");
      	
      	statisticCalculator.doSentimentAnalyst(sparkConfig, weekPostRDD, "WEEK", "POST");
      	statisticCalculator.doSentimentAnalyst(sparkConfig, weekCommentRDD, "WEEK", "COMMENT");
    	
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
