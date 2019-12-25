package vn.uit.edu.sa.backEndService;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;

import com.mongodb.MongoClient;

import scala.Function1;
import vn.uit.edu.sa.connectDB.ConnectMongoDB;
import vn.uit.edu.sa.connectDB.MongoSparkHelper;
import vn.uit.edu.sa.dto.DataFrameToRDDConvertor;
import vn.uit.edu.sa.dto.DTO;
import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.ConfigReader;
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
        
        MongoClient mongoDB = ConnectMongoDB.ConnectMongoDB(ConfigReader.readConfig("local.db.url"), Integer.valueOf(ConfigReader.readConfig("local.db.port")));
        
        ConnectMongoDB mongoDBConnection = new ConnectMongoDB(mongoDB);
        
        mongoDBConnection.addOrUpdateNewDocument(null);
       
        
//	    SparkConfigure sparkConfig = new SparkConfigure();
//	    MongoSparkHelper mongod = null;
	    
//	    if (args[0].equals("remote")){
//    	mongod = new MongoSparkHelper(sparkConfig, true, "post");
//    }else if (args[0].equals("local")){
//    	mongod = new MongoSparkHelper(sparkConfig, false, "post");
//    }
    
//    mongod = new MongoSparkHelper(sparkConfig, true, "post");

	    	
//	DataFrame postDF = mongod.read("post");   
//	DataFrame commentDF = mongod.read("comment");
//	int numOfPostDFColumns = postDF.columns().length;
//	int numOfCommentDFColumns = commentDF.columns().length;
//	postDF.show();
//	commentDF.show();
	
		
	//comment
	//createdDate:04
	//message:05
	//postId: 07
	
	//post: postedByUserId
	//post: _ids
	
//	String param = "01-01-2019";
	
	//Run on the first time
//	JavaRDD<DTO> postRDD =  DataFrameToRDDConvertor.convertFromDataFrameToPostDTO(postDF, numOfPostDFColumns);
//	JavaRDD<DTO> commentRDD = DataFrameToRDDConvertor.convertFromDataFrameToCommentDTO(commentDF, numOfCommentDFColumns);
	
//	DataFilter filter = new DataFilter();
//  	StatisticCalculator statisticCalculator = new StatisticCalculator();
//        StatisticCalculator statisticCalculator = new StatisticCalculator(mongoDBConnection.getData());

	//post filter must be run first to avoid empty postIds list.
//	JavaRDD<DTO> monthPostRDD = filter.postDTOFilterFactory(postRDD, new String[] {param});
//	JavaRDD<DTO> monthCommentRDD = filter.commentDTOFilterFactory(commentRDD, new String[] {param});
//	JavaRDD<DTO> weekPostRDD = filter.weekPostDTPFilterFactory(postRDD, new String[] {param});
//	JavaRDD<DTO> weekCommentRDD = filter.weekCommentDTPFilterFactory(commentRDD, new String[] {param});
	
//  	statisticCalculator.doSentimentAnalyst(sparkConfig, monthPostRDD, "MONTH", "POST");
//  	statisticCalculator.doSentimentAnalyst(sparkConfig, monthCommentRDD, "MONTH", "COMMENT");

//	if (weekPostRDD == null)
//		System.out.println("There are no records in post from last 7 days!");
//	else {
//	  	statisticCalculator.doSentimentAnalyst(sparkConfig, weekPostRDD, "WEEK", "POST");
//	}
//	
//	if (weekCommentRDD == null)
//		System.out.println("There are no records in comment from last 7 days!");
//	else {
//	  	statisticCalculator.doSentimentAnalyst(sparkConfig, weekCommentRDD, "WEEK", "COMMENT");
//	}

  	//statisticCalculator.doSentimentAnalyst(sparkConfig, weekCommentRDD, "WEEK", "COMMENT");
  	
  	//statisticCalculator.show();
        
    //statisticCalculator.updateToDB(mongoDB);
    }
}
