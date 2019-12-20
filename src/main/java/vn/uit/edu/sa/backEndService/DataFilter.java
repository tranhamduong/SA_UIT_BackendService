package vn.uit.edu.sa.backEndService;

import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import vn.uit.edu.sa.dto.DTO;

public class DataFilter {
	private List<String> universityFanpageIds;
	
	private List<String> universityGroupIds;
	private List<String> postIds;
	
	private Date date = null;

	
	public DataFilter() {
		postIds = new ArrayList<String>();
	}
	
	public JavaRDD<DTO> postDTOFilterFactory(JavaRDD<DTO> rdd, String[] parameters){		
		date = null;
    	DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
		try {
			date = new java.sql.Date(dateFormat.parse(parameters[0]).getTime());

		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		rdd = rdd.filter(new Function<DTO, Boolean>() {
			
			@Override
			public Boolean call(DTO dto) throws Exception {
				if (universityFanpageIds.contains(dto.getPostedByUserId()) || universityGroupIds.contains(dto.getPostedByUserId())) {
					if (dto.getCreatedDate().compareTo(date) > 0) {
						postIds.add(dto.getPostId());
						return true;
					}				
				}
				return false;
			}
		});
		
		return rdd;
	}
	
	public JavaRDD<DTO> commentDTOFilterFactory(JavaRDD<DTO> rdd, String[] parameters){
		
		date = null;
    	DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
		try {
			date = new java.sql.Date(dateFormat.parse(parameters[0]).getTime());

		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		rdd = rdd.filter(new Function<DTO, Boolean>() {
			
			@Override
			public Boolean call(DTO dto) throws Exception {
				if (postIds.contains(dto.getPostId())) {
					if (dto.getCreatedDate().compareTo(date) > 0)
						return true;
				}
				return false;
			}
		});
		
		return rdd;
	}

	public JavaRDD<DTO> weekPostDTPFilterFactory(JavaRDD<DTO> rdd, String[] parameters) {
		date = null;
    	
    	DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
    	
		try {
			date = new java.sql.Date(dateFormat.parse(parameters[0]).getTime());

		} catch (ParseException e) {
			e.printStackTrace();
		}
	
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, -6);
			
    	rdd = rdd.filter(new Function<DTO, Boolean>() {
			
			@Override
			public Boolean call(DTO dto) throws Exception {
				if (dto.getCreatedDate().before(date) && dto.getCreatedDate().after(cal.getTime())){
					return true;
				}
				return false;
			}
		});
    	
		return rdd;
	}
}
