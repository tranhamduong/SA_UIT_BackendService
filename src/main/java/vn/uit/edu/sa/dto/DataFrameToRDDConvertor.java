package vn.uit.edu.sa.dto;

import java.sql.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class DataFrameToRDDConvertor {
	
	public static JavaRDD<DTO> convertFromDataFrameToPostDTO(DataFrame df) {
		JavaRDD<DTO> rdd = null;
		rdd = df.toJavaRDD().map(new Function<Row, DTO>() {

			@Override
			public DTO call(Row row) throws Exception {
				DTO post = new DTO();

				if (!row.isNullAt(19)) {
					if (!row.getString(19).equals("NOTE") && !row.getString(19).equals("VIDEO") && !row.getString(19).equals("EVENT")){
						post.setPostType(row.getString(19));
					}else return null;
				} else return null;
				
//				if (!row.isNullAt(7))
//					post.setCreatedDate(Date.valueOf(row.getTimestamp(7).toLocalDateTime().toLocalDate()));
//				else return null;
				
				if (!row.isNullAt(7)) {
					post.setCreatedDate(Date.valueOf(row.getTimestamp(7).toLocalDateTime().toLocalDate()));
					post.setMonth(post.getCreatedDate().getMonth());
				}
				else return null;
				
				
				if (!row.isNullAt(13) && !row.getString(13).equals(" ")) 
					post.setMessage(row.getString(13));
				else return null;
				
				if (!row.isNullAt(0)) 
					post.setPostId(row.getString(0));
				else return null;
				
				if (!row.isNullAt(1))
					post.setPostedByUserId(row.getString(1));
				else return null;
				
				return post;
			}
		});
		
		rdd = rdd.filter(new Function<DTO, Boolean>() {
		
			@Override
			public Boolean call(DTO v1) throws Exception {
				return v1 != null;
			}
		});
		return rdd;
	}
	
	public static JavaRDD<DTO> convertFromDataFrameToCommentDTO(DataFrame df){
		JavaRDD<DTO> rdd = null;
		rdd = df.toJavaRDD().map(new Function<Row, DTO>() {

			@Override
			public DTO call(Row row) throws Exception {
				DTO post = new DTO();
				
				if (!row.isNullAt(7)) 
					post.setPostId(row.getString(7));
				else return null;
				
				if (!row.isNullAt(4)) {
					post.setCreatedDate(Date.valueOf(row.getTimestamp(7).toLocalDateTime().toLocalDate()));					
					post.setMonth(post.getCreatedDate().getMonth());
				}else return null;
				
				if (!row.isNullAt(5) && !row.getString(13).equals(" ")) {
					post.setMessage(row.getString(13));
				} else return null;
				return post;
			}
		});
		
		rdd = rdd.filter(new Function<DTO, Boolean>() {
		
			@Override
			public Boolean call(DTO v1) throws Exception {
				return v1 != null;
			}
		});
		return rdd;
	}
}
