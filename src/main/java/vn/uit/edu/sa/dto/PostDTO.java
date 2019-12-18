package vn.uit.edu.sa.dto;

import java.sql.Date;

public class PostDTO implements java.io.Serializable {
	@Override
	public String toString() {
		return "PostDTO [createdDate=" + createdDate + ", postType=" + postType + "]";
	}
	public Date getCreatedDate() {
		return createdDate;
	}
	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}
	private String message;
	private Date createdDate;
	private String postType;
	
	
	public PostDTO(String message, Date createdDate, String postType) {
		super();
		this.message = message;
		this.createdDate = createdDate;
		this.postType = postType;
	}
	public PostDTO() {
		super();
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}

	public String getPostType() {
		return postType;
	}
	public void setPostType(String postType) {
		this.postType = postType;
	}
	
}
