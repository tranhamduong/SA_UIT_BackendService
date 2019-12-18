package vn.uit.edu.sa.dto;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

public class Statistic implements Serializable {
	@Override
	public String toString() {
		return "Statistic [type=" + type + ", typeDetail=" + typeDetail + ", posTraining=" + posTraining
				+ ", negTraining=" + negTraining + ", posFacility=" + posFacility + ", negFacility=" + negFacility
				+ "]";
	}

	private String type = "";
	public Statistic(String type, String typeDetail, String typeSource) {
		super();
		this.type = type;
		this.typeDetail = typeDetail;
		this.typeSource = typeSource;
	}
	
	public void setType(String type) {
		this.type = type;
	}

	private String typeDetail = ""; // 
	private String typeSource = ""; //Comment or Post
	private String posTraining = "0";
	private String negTraining = "0";
	private String posFacility = "0";
	private String negFacility = "0";
	
	public Statistic(String type, String typeDetail, String typeSource, String posTraining, String negTraining,
			String posFacility, String negFacility) {
		super();
		this.type = type;
		this.typeDetail = typeDetail;
		this.typeSource = typeSource;
		this.posTraining = posTraining;
		this.negTraining = negTraining;
		this.posFacility = posFacility;
		this.negFacility = negFacility;
	}

	public Statistic() {
		super();
	}

	public String getTypeDetail() {
		return typeDetail;
	}

	public void setTypeDetail(String typeDetail) {
		this.typeDetail = typeDetail;
	}

	public String getTypeSource() {
		return typeSource;
	}

	public void setTypeSource(String typeSource) {
		this.typeSource = typeSource;
	}

	public String getPosTraining() {
		return posTraining;
	}

	public void setPosTraining(String posTraining) {
		this.posTraining = posTraining;
	}

	public String getNegTraining() {
		return negTraining;
	}

	public void setNegTraining(String negTraining) {
		this.negTraining = negTraining;
	}

	public String getPosFacility() {
		return posFacility;
	}

	public void setPosFacility(String posFacility) {
		this.posFacility = posFacility;
	}

	public String getNegFacility() {
		return negFacility;
	}

	public void setNegFacility(String negFacility) {
		this.negFacility = negFacility;
	}

	public String getType() {
		return type;
	}
	
	
}
