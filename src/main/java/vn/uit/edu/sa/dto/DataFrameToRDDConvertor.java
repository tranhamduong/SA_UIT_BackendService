package vn.uit.edu.sa.dto;

import java.sql.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import scala.Function1;

public class DataFrameToRDDConvertor {
	
	public static JavaRDD<DTO> convertFromDataFrameToPostDTO(DataFrame df, int numOfPostDFColumns) {
		JavaRDD<DTO> rdd = null;
		
		if (numOfPostDFColumns == 33) {
			rdd = df.toJavaRDD().map(new Function<Row, DTO>() {

				@Override
				public DTO call(Row row) throws Exception {
					DTO post = new DTO();
					
					
					if (!row.isNullAt(14) && !row.getString(14).equals(" ")) 
						post.setMessage(row.getString(14));
					else return null;
				
					if (row.isNullAt(10)) {
						if (row.isNullAt(20)) return null;
						
						switch(row.getString(20)) {
						case "NOTE": return null;
						case "EVENT":  return null;
						case "LINK":  return null;
						case "VIDEO":  return null;
						case "PHOTO": {
							post.setPostType("POST");
						}break;
						case "STATUS": { 
							post.setPostType("POST");
						}break;
						default: {
							return null;
						}
						}	
					}else {
						post.setPostType("GROUP");
						post.setGroupId(row.getString(10));	
					}

					
					if (!row.isNullAt(7)) {
						post.setCreatedDate(Date.valueOf(row.getTimestamp(7).toLocalDateTime().toLocalDate()));
						post.setMonth(post.getCreatedDate().getMonth());
					}
					else return null;
					
					if (!row.isNullAt(1)) 
						post.setPostId(row.getString(1));
					else return null;
					
					if (!row.isNullAt(22))
						post.setPostedByUserId(row.getString(22));
					else return null;
					
					return post;
				}
			});
		}else { //columns: 41
			System.out.println("EXCEPTON HAPPEND IN DATAFRAME CONVERTOR!!!");
			rdd = df.toJavaRDD().map(new Function<Row, DTO>() {

				@Override
				public DTO call(Row row) throws Exception {
					DTO post = new DTO();
					
					
					if (row.isNullAt(12)) {
						switch(row.getString(27)) {
						case "NOTE": return null;
						case "EVENT":  return null;
						case "LINK":  return null;
						case "VIDEO":  return null;
						case "PHOTO": {
							post.setPostType("POST");
						}break;
						case "STATUS": { 
							post.setPostType("POST");
						}break;
						default: {
							return null;
						}
						}	
					}else {
						post.setPostType("GROUP");
						post.setGroupId(row.getString(12));	
					}
					
					if (!row.isNullAt(9)) {
						post.setCreatedDate(Date.valueOf(row.getTimestamp(9).toLocalDateTime().toLocalDate()));
						post.setMonth(post.getCreatedDate().getMonth());
					}
					else return null;
					
					
					if (!row.isNullAt(17) && !row.getString(17).equals(" ")) 
						post.setMessage(row.getString(17));
					else return null;
					
					if (!row.isNullAt(1)) 
						post.setPostId(row.getString(1));
					else return null;
					
					if (!row.isNullAt(29))
						post.setPostedByUserId(row.getString(29));
					else return null;
					
					return post;
				}
			});
		}
		
		rdd = rdd.filter(new Function<DTO, Boolean>() {
		
			@Override
			public Boolean call(DTO v1) throws Exception {
				return v1 != null;
			}
		});
		return rdd;
	}
	
	public static JavaRDD<DTO> convertFromDataFrameToCommentDTO(DataFrame df, int numOfCommentDFColumns){
		JavaRDD<DTO> rdd = null;
		
		if (numOfCommentDFColumns == 15) {
			rdd = df.toJavaRDD().map(new Function<Row, DTO>() {

				@Override
				public DTO call(Row row) throws Exception {
					DTO post = new DTO();
					
					if (!row.isNullAt(9)) 
						post.setPostId(row.getString(7));
					else return null;
					
					if (!row.isNullAt(5)) {
						post.setCreatedDate(Date.valueOf(row.getTimestamp(5).toLocalDateTime().toLocalDate()));					
						post.setMonth(post.getCreatedDate().getMonth());
					}else return null;
					
					if (!row.isNullAt(7) && !row.getString(7).equals(" ")) {
						post.setMessage(row.getString(7));
					} else return null;
					
					post.setPostType("COMMENT");
					
					return post;
				}
			});
		}else { //columns: 13
			rdd = df.toJavaRDD().map(new Function<Row, DTO>() {

				@Override
				public DTO call(Row row) throws Exception {
					DTO post = new DTO();
					
					if (!row.isNullAt(7)) 
						post.setPostId(row.getString(7));
					else return null;
					
					if (!row.isNullAt(4)) {
						post.setCreatedDate(Date.valueOf(row.getTimestamp(4).toLocalDateTime().toLocalDate()));					
						post.setMonth(post.getCreatedDate().getMonth());
					}else return null;
					
					if (!row.isNullAt(5) && !row.getString(5).equals(" ")) {
						post.setMessage(row.getString(5));
					} else return null;
					
					post.setPostType("COMMENT");
					
					return post;
				}
			});
		}
		

		
		rdd = rdd.filter(new Function<DTO, Boolean>() {
		
			@Override
			public Boolean call(DTO v1) throws Exception {
				return v1 != null;
			}
		});
		return rdd;
	}
}
