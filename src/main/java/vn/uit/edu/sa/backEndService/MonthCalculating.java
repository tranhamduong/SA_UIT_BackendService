package vn.uit.edu.sa.backEndService;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import vn.uit.edu.sa.dto.PostDTO;
import vn.uit.edu.sa.dto.Statistic;
import vn.uit.edu.sa.languagePreprocessor.LanguagePreprocessor;
import vn.uit.edu.sa.model.SentimentAnalyser;
import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.ConfigReader;
import vn.uit.edu.sa.util.RDDutils;

public class MonthCalculating  implements Serializable{
	private Statistic Jan;
	private Statistic Feb;
	private Statistic Mar;
	private Statistic Apr;
	private Statistic May;
	private Statistic Jun;
	private Statistic Jul;
	private Statistic Aug;
	private Statistic Sep;
	private Statistic Oct;
	private Statistic Nov;
	private Statistic Dec;
	
	List<Statistic> listMonth;
	
	JavaRDD<PostDTO> listJan;
	JavaRDD<PostDTO> listFeb;
	JavaRDD<PostDTO> listMar;
	JavaRDD<PostDTO> listApr;
	JavaRDD<PostDTO> listMay;
	JavaRDD<PostDTO> listJun;
	JavaRDD<PostDTO> listJul;
	JavaRDD<PostDTO> listAug;
	JavaRDD<PostDTO> listSep;
	JavaRDD<PostDTO> listOct;
	JavaRDD<PostDTO> listNov;
	JavaRDD<PostDTO> listDec;
	
	JavaRDD<String> listStrJan;
	JavaRDD<String> listStrFeb;
	JavaRDD<String> listStrMar;
	JavaRDD<String> listStrApr;
	JavaRDD<String> listStrMay;
	JavaRDD<String> listStrJun;
	JavaRDD<String> listStrJul;
	JavaRDD<String> listStrAug;
	JavaRDD<String> listStrSep;
	JavaRDD<String> listStrOct;
	JavaRDD<String> listStrNov;
	JavaRDD<String> listStrDec;
	
	List<JavaRDD<PostDTO>> listMonthRdd;
	
	List<JavaRDD<String>> listMonthStringRdd;

	int posTraining = 0;
	int negTraining = 0;
	int posFacility = 0;
	int negFacility = 0;
	
	public MonthCalculating() {
		
		listMonth = new ArrayList<Statistic>();
		
		listMonthRdd = new ArrayList<JavaRDD<PostDTO>>();
	}
	
	public void doSentimentAnalysFollowMonth(SparkConfigure spark, JavaRDD<PostDTO> rdd) {
			
		listJan = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 0)
					return true;
				return false;
			}
		});
		
		listFeb = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 1)
					return true;
				return false;
			}
		});
		
		listMar = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 2)
					return true;
				return false;
			}
		});
		
		listApr = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 3)
					return true;
				return false;
			}
		});
		
		listMay = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 4)
					return true;
				return false;
			}
		});
		
		listJun = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 5)
					return true;
				return false;
			}
		});
		
		listJul = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 6)
					return true;
				return false;
			}
		});
		
		listAug = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 7)
					return true;
				return false;
			}
		});
		
		listSep = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 8)
					return true;
				return false;
			}
		});
		
		listOct = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 9)
					return true;
				return false;
			}
		});
		
		listNov = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 10)
					return true;
				return false;
			}
		});
		
		listDec = rdd.filter(new Function<PostDTO, Boolean>() {
			
			@SuppressWarnings("deprecation")
			@Override
			public Boolean call(PostDTO v1) throws Exception {
				if (v1.getCreatedDate().getMonth() == 11)
					return true;
				return false;
			}
		});
		
		
		listMonthRdd.add(listJan);		listMonthRdd.add(listFeb);
		listMonthRdd.add(listMar);		listMonthRdd.add(listApr);
		listMonthRdd.add(listMay);		listMonthRdd.add(listJun);
		listMonthRdd.add(listJul);		listMonthRdd.add(listAug);
		listMonthRdd.add(listSep);		listMonthRdd.add(listOct);
		listMonthRdd.add(listNov);		listMonthRdd.add(listDec);
		
		
		
		LanguagePreprocessor.cleanDir();
		LanguagePreprocessor preProcessor = new LanguagePreprocessor(spark, "all");
		int month = 0;
		for (JavaRDD<PostDTO> postRdd : listMonthRdd) {
			if (postRdd.count() > 0) {
				switch(month) {
				case 0:{
					listStrJan = preProcessor.run(postRdd);
					break;
				}
				case 1:{
					listStrFeb = preProcessor.run(postRdd);
					break;
				}
				case 2:{
					listStrMar = preProcessor.run(postRdd);
					break;
				}
				case 3:{
					listStrApr = preProcessor.run(postRdd);
					break;
				}
				case 4:{
					listStrMay = preProcessor.run(postRdd);
					break;
				}
				case 5:{
					listStrJun = preProcessor.run(postRdd);
					break;
				}
				case 6:{
					listStrJul = preProcessor.run(postRdd);
					break;
				}
				case 7:{
					listStrAug = preProcessor.run(postRdd);
					break;
				}
				case 8:{
					listStrSep = preProcessor.run(postRdd);
					break;
				}
				case 9:{
					listStrOct = preProcessor.run(postRdd);
					break;
				}
				case 10:{
					listStrNov = preProcessor.run(postRdd);
					break;
				}
				case 11:{
					listStrDec = preProcessor.run(postRdd);
					break;
				}
				}
			}else
				System.out.println("The month " + getMonth(month) + " has no entries.");
			month++;
		}
		
		listMonthStringRdd.add(listStrJan);		listMonthStringRdd.add(listStrFeb);
		listMonthStringRdd.add(listStrMar);		listMonthStringRdd.add(listStrApr);
		listMonthStringRdd.add(listStrMay);		listMonthStringRdd.add(listStrJun);
		listMonthStringRdd.add(listStrJul);		listMonthStringRdd.add(listStrAug);
		listMonthStringRdd.add(listStrSep);		listMonthStringRdd.add(listStrOct);
		listMonthStringRdd.add(listStrNov);		listMonthStringRdd.add(listStrDec);
		
		doSentiment();
	}
	
	private void doSentiment() {
		System.out.println("CHECKPOINT");
		//false if in Training mode
		try {
			SentimentAnalyser model = new SentimentAnalyser(false);
			int month = 0;
			for (JavaRDD<String> strMonth : listMonthStringRdd) {

				
				strMonth.foreach(new VoidFunction<String>() {

					@Override
					public void call(String str) throws Exception {
						double[] result = model.testSample(str);
						if (result[0] > result[1]) {
							if (result[2] > result[3]) {
								posTraining++;
							}else {
								negTraining++;
							}
						}else {
							if (result[2] > result[3]) {
								posFacility++;
							}else {
								negFacility++;
							}
						}
					}
				});		
				
				if (month == 0) 
					Jan = new Statistic("MONTH", "Jan", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 1)
					Feb = new Statistic("MONTH", "Feb", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 2)
					Mar = new Statistic("MONTH", "Mar", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 3)
					Apr = new Statistic("MONTH", "Apr", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 4)
					May = new Statistic("MONTH", "May", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 5)
					Jun = new Statistic("MONTH", "Jun", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 6)
					Jul = new Statistic("MONTH", "Jul", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 7)
					Aug = new Statistic("MONTH", "Aug", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 8)
					Sep = new Statistic("MONTH", "Sep", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 9)
					Oct = new Statistic("MONTH", "Oct", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 10)
					Nov = new Statistic("MONTH", "Nov", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				else if(month == 11)
					Dec = new Statistic("MONTH", "Dec", "Post", String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
				
				month++;
				posTraining = 0; 			negTraining = 0;
				posFacility = 0;			negFacility = 0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		listMonth.add(Jan);			listMonth.add(Feb);
		listMonth.add(Mar);			listMonth.add(Apr);
		listMonth.add(May);			listMonth.add(Jun);
		listMonth.add(Jul);			listMonth.add(Aug);
		listMonth.add(Sep);			listMonth.add(Oct);
		listMonth.add(Nov);			listMonth.add(Dec);
		
		show();
	}
	
	private String getMonth(int month) {
		if (month == 0) return "Jan";
		if (month == 1) return "Feb";
		if (month == 2) return "Mar";
		if (month == 3) return "Apr";
		if (month == 4) return "May";
		if (month == 5) return "Jun";
		if (month == 6) return "Jul";
		if (month == 7) return "Aug";
		if (month == 8) return "Sep";
		if (month == 9) return "Oct";
		if (month == 10) return "Nov";
		return "Dec";
	}

	public void showRDD() {
		if (listMonthRdd == null) System.out.println("checkpoint");
		int i = 0;
			for (JavaRDD<PostDTO> rdd : listMonthRdd) {
				System.out.println(i + "/" + rdd.count());
//				if (rdd != null) {
//					RDDutils.show(rdd);
//				}
				i++;
			}
	}
	
	public void show() {
		for(Statistic month : listMonth) {
			System.out.println(month.getTypeDetail() + " : Training: " + month.getPosTraining() + "-" + month.getNegTraining());
			System.out.println(month.getTypeDetail() + " : Facility: " + month.getPosFacility() + "-" + month.getNegFacility());
			System.out.println("===========================");
		}
	}
}
