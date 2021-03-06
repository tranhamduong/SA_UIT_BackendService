package vn.uit.edu.sa.dto;

import java.io.Serializable;
import java.sql.Date;

public class DTO implements Serializable{
	
	private String postId;
	private String postType; 
	private String postedByUserId;
	private String message;
	private Date createdDate;
	private int month;
	private String groupId;
	private String dayOfWeek;
	
	public String getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(String dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public DTO(String postId, String postType, String postedByUserId, String message, Date createdDate, int month,
			String groupId) {
		super();
		this.postId = postId;
		this.postType = postType;
		this.postedByUserId = postedByUserId;
		this.message = message;
		this.createdDate = createdDate;
		this.month = month;
		this.groupId = groupId;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public DTO(String postId, String postType, String postedByUserId, String message, Date createdDate, int month) {
		super();
		this.postId = postId;
		this.postType = postType;
		this.postedByUserId = postedByUserId;
		this.message = message;
		this.createdDate = createdDate;
		this.month = month;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public DTO() {
		super();
	}

	public DTO(String postId, String message, Date createdDate, String postType, String postedByUserId) {
		super();
		this.postId = postId;
		this.message = message;
		this.createdDate = createdDate;
		this.postType = postType;
		this.postedByUserId = postedByUserId;
	}

	@Override
	public String toString() {
		return "DTO [createdDate=" + createdDate + ", postType = " + this.postType + ", postId = " + this.postId + "]";
	}

	public String getPostId() {
		return postId;
	}

	public void setPostId(String postId) {
		this.postId = postId;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Date getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}

	public String getPostType() {
		return postType;
	}

	public void setPostType(String postType) {
		this.postType = postType;
	}

	public String getPostedByUserId() {
		return postedByUserId;
	}

	public void setPostedByUserId(String postedByUserId) {
		this.postedByUserId = postedByUserId;
	}	
}
