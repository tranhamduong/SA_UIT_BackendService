package vn.uit.edu.sa.dto;

import java.sql.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class DataFrameToRDDConvertor {
	public static JavaRDD<PostDTO> convertFromDataFrame(DataFrame df) {
		JavaRDD<PostDTO> rdd = null;
		rdd = df.toJavaRDD().map(new Function<Row, PostDTO>() {

			@Override
			public PostDTO call(Row row) throws Exception {
				PostDTO post = new PostDTO();

				if (!row.isNullAt(19)) {
					if (!row.getString(19).equals("NOTE") && !row.getString(19).equals("VIDEO") && !row.getString(19).equals("EVENT")){
						post.setPostType(row.getString(19));
					}else return null;
				} else return null;
				
				if (!row.isNullAt(7)) {
					post.setCreatedDate(Date.valueOf(row.getTimestamp(7).toLocalDateTime().toLocalDate()));
				}else return null;
				
				if (!row.isNullAt(13) && !row.getString(13).equals(" ")) {
					post.setMessage(row.getString(13));
				} else return null;
				return post;
			}
		});
		
		rdd = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				return v1 != null;
			}
		});
		
		return rdd;
	}
}
