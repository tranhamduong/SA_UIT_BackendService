package vn.uit.edu.sa.util;

import java.sql.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import vn.uit.edu.sa.dto.DTO;

public class RDDutils{
	public static JavaRDD<DTO> getOnlyNewPostFromDate(JavaRDD<DTO> rdd,final Date date){
		System.out.println(rdd);
		
		rdd = rdd.filter(new Function<DTO, Boolean>() {
			
			@Override
			public Boolean call(DTO v1) throws Exception {
				return v1 != null;
			}
		});
		

		rdd = rdd.filter(new Function<DTO, Boolean>() {
			
			@Override
			public Boolean call(DTO dto) throws Exception {
				if (dto.getMessage() == null || dto.getCreatedDate() == null || dto.getPostType() == null)
					return false;
				else return true;
			}
		});
		
		rdd = rdd.filter(new Function<DTO, Boolean>() {
			
			@Override
			public Boolean call(DTO dto) throws Exception {
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
	
	public static JavaRDD<String> convertFromDTOtoString(JavaRDD<DTO> rdd){
		
		JavaRDD<String> newRdd = rdd.map(new Function<DTO, String>() {

			@Override
			public String call(DTO dto) throws Exception {
				return dto.getMessage();
			}
		});
		return newRdd;
	}
	
	public static void show(JavaRDD<DTO> rdd) {
		List<DTO> list = rdd.collect();

		for (DTO dto : list) 
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
	
	public static JavaRDD<DTO> removeEmptyRow(JavaRDD<DTO> rdd){
		rdd = rdd.filter(new Function<DTO, Boolean>() {
			
			@Override
			public Boolean call(DTO v1) throws Exception {
				if (v1.getMessage().isEmpty() || v1.getMessage().equals(" ") || v1.getMessage() == null || v1.getMessage() == "\n" || v1.getMessage().equals(System.lineSeparator()))
					return false;
				return true;
			}
		});
		return rdd;
	}
}
