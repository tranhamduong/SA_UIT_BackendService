package vn.uit.edu.sa.util;

import java.sql.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import vn.uit.edu.sa.dto.DTO;
import vn.uit.edu.sa.dto.PostDTO;


public class RDDutils{
	public static JavaRDD<PostDTO> getOnlyNewPostFromDate(JavaRDD<PostDTO> rdd,final Date date){
		System.out.println(rdd);
		
		rdd = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				// TODO Auto-generated method stub
				return v1 != null;
			}
		});
		

		rdd = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@Override
			public Boolean call(PostDTO dto) throws Exception {
				if (dto.getMessage() == null || dto.getCreatedDate() == null || dto.getPostType() == null)
					return false;
				else return true;
			}
		});
		
		rdd = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@Override
			public Boolean call(PostDTO dto) throws Exception {
				if (dto.getCreatedDate().compareTo(date) < 0) {
					return false;
				}
				else {
					return true;
				}
			}
		});
		
		return rdd;
	}
	
	public static JavaRDD<String> convertFromPostDTOtoString(JavaRDD<PostDTO> rdd){
		
		JavaRDD<String> newRdd = rdd.map(new Function<PostDTO, String>() {

			@Override
			public String call(PostDTO dto) throws Exception {
				return dto.getMessage();
			}
		});
		return newRdd;
	}
	
	public static void show(JavaRDD<PostDTO> rdd) {
		List<PostDTO> list = rdd.collect();

		for (PostDTO dto : list) 
		{
			System.out.println(dto.toString());
		}
	}
	
	public static void showStringRDD(JavaRDD<String> rdd) {
		List<String> list = rdd.collect();

		System.out.println(list.size());
		for (String dto : list) 
		{
			System.out.println(dto);
		}
	}
	
	public static JavaRDD<String> removeEmptyRow(JavaRDD<String> rdd){
		return rdd.filter(new Function<String, Boolean>() {
			
			@Override
			public Boolean call(String v1) throws Exception {
				if (v1.isEmpty() || v1.equals(" ") || v1 == null || v1 == "\n" || v1.equals(System.lineSeparator()))
					return false;
				return true;
			}
		});
	}
}
