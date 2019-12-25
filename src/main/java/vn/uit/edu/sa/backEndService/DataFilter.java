package vn.uit.edu.sa.backEndService;

import java.io.Serializable;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import vn.uit.edu.sa.dto.DTO;
import vn.uit.edu.sa.util.HelpFunction;
import vn.uit.edu.sa.util.RDDutils;

public class DataFilter implements Serializable {
	private List<String> universityFanpageIds;
	
	private List<String> universityGroupIds;
	private static List<String> postIds;
	
	private Date date = null;

	
	public DataFilter() {
		postIds = new ArrayList<String>();
		universityFanpageIds = HelpFunction.getUniversityFanpageIdList();
		
		universityGroupIds = HelpFunction.getUniversityGroupIdList();
	}
	
	public List<String> getPostIds() {
		return postIds;
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
					
					if (dto.getPostType().equals("GROUP")) {
						if (universityGroupIds.contains((dto.getGroupId()))){
							if (dto.getCreatedDate().compareTo(date) > 0) {
								dto.setPostType("POST");
								if (!postIds.contains(dto.getPostId())) {
									postIds.add(dto.getPostId());
								}
								return true;
							}
						}
					}else {
						if (universityFanpageIds.contains(dto.getPostedByUserId())) {
						if (dto.getCreatedDate().compareTo(date) > 0) {
							if (!postIds.contains(dto.getPostId())) {
								postIds.add(dto.getPostId());
							}
							return true;
						}				
					}
					}
					return false;
				}
			});
		

		
		rdd = RDDutils.removeEmptyRow(rdd);

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
		
    	JavaRDD<DTO> result;
    	DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
    	
		try {
			date = new java.sql.Date(dateFormat.parse(parameters[0]).getTime());

		} catch (ParseException e) {
			e.printStackTrace();
		}
	
		final Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, -6);
		
		result = rdd.filter(new Function<DTO, Boolean>() {
			
			@Override
			public Boolean call(DTO dto) throws Exception {
				if (universityFanpageIds.contains(dto.getPostedByUserId())) {
					if (dto.getCreatedDate().before(date) && dto.getCreatedDate().after(cal.getTime())) {
						dto.setDayOfWeek(HelpFunction.getDayOfWeek(dto.getCreatedDate()));
						if (!postIds.contains(dto.getPostId()))
							postIds.add(dto.getPostId());
						return true;
					}				
				}
				if (dto.getPostType().equals("GROUP") && dto.getGroupId()!= null) {
					if (universityGroupIds.contains((dto.getGroupId()))){
						if (dto.getCreatedDate().before(date) && dto.getCreatedDate().after(cal.getTime())) {
							dto.setDayOfWeek(HelpFunction.getDayOfWeek(dto.getCreatedDate()));
							if (!postIds.contains(dto.getPostId()))
								postIds.add(dto.getPostId());
							System.out.println("GROUP");
							return true;
						}
					}
				}
				return false;
			}
		});
    	
    	if (result.count() == 0)
    		return null;
    	
    	return result;
	}
	
	public JavaRDD<DTO> weekCommentDTPFilterFactory(JavaRDD<DTO> rdd, String[] parameters) {
		date = null;
		
    	JavaRDD<DTO> result;
    	DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
    	
		try {
			date = new java.sql.Date(dateFormat.parse(parameters[0]).getTime());

		} catch (ParseException e) {
			e.printStackTrace();
		}
	
		final Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, -6);
		
		result = rdd.filter(new Function<DTO, Boolean>() {
			
			@Override
			public Boolean call(DTO dto) throws Exception {
				if (postIds.contains(dto.getPostId())) {
					if (dto.getCreatedDate().before(date) && dto.getCreatedDate().after(cal.getTime())){
						dto.setDayOfWeek(HelpFunction.getDayOfWeek(dto.getCreatedDate()));
						return true;
					}
				}
				return false;
			}
		});
    	
    	if (result.count() == 0)
    		return null;
    	
    	return result;
	}
}