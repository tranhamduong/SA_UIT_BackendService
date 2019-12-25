package vn.uit.edu.sa.backEndService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import akka.japi.Util;
import vn.uit.edu.sa.connectDB.ConnectMongoDB;
import vn.uit.edu.sa.dto.DTO;
import vn.uit.edu.sa.dto.Statistic;
import vn.uit.edu.sa.languagePreprocessor.LanguagePreprocessor;
import vn.uit.edu.sa.model.SentimentAnalyser;
import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.ConfigReader;
import vn.uit.edu.sa.util.HelpFunction;
import vn.uit.edu.sa.util.RDDutils;

public class StatisticCalculator  implements Serializable{
	
	private Statistic stat;
	
	private double[] result;
	
	public static List<Statistic> listMonth;
	public List<Statistic> resultList;
	public List<Statistic> tempList;
	
	JavaRDD<DTO> monthRDD;
	
	List<JavaRDD<DTO>> listMonthRdd;
	
	List<JavaRDD<String>> listMonthStringRdd;

	int posTraining = 0;
	int negTraining = 0;
	int posFacility = 0;
	int negFacility = 0;
	
	SentimentAnalyser model ;
	
	public StatisticCalculator() {
		
		listMonth = new ArrayList<Statistic>();
		resultList = new ArrayList<Statistic>();
		tempList = new ArrayList<Statistic>();
		
		listMonthRdd = new ArrayList<JavaRDD<DTO>>();
		
		monthRDD = null;
	}
	
	public StatisticCalculator(List<Statistic> list) {
		listMonth = new ArrayList<Statistic>();
		resultList = new ArrayList<Statistic>();
		tempList = new ArrayList<Statistic>();
		
		listMonthRdd = new ArrayList<JavaRDD<DTO>>();
		
		monthRDD = null;
		
		listMonth = list;
	}
	
	public void doSentimentAnalystWithAlreadyData(SparkConfigure spark, JavaRDD<DTO> rdd, String type, String typeSource, List<Statistic> list) {
		listMonth = list;
		doSentimentAnalyst(spark, rdd, type, typeSource);
	}
	
	
	public void doSentimentAnalyst(SparkConfigure spark, JavaRDD<DTO> rdd, final String type, final String typeSource) {
		listMonth = new ArrayList<Statistic>();
		try {
			model = new SentimentAnalyser(false);
			LanguagePreprocessor preProcessor = new LanguagePreprocessor(spark);
			
			rdd = preProcessor.run(rdd);
			
			rdd.foreach(new VoidFunction<DTO>() {
				
				@Override
				public void call(DTO dto) throws Exception {
					result = null;
					stat = null;
					result = model.testSample(dto.getMessage());
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
										
					if (listMonth.size() == 0) {
						if (type.equals("MONTH"))
							stat = new Statistic(type, HelpFunction.getMonth(dto.getMonth()), typeSource, String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
						else if (type.equals("WEEK"))
							stat = new Statistic(type, dto.getDayOfWeek(), typeSource, String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
						listMonth.add(stat);
					}
					Boolean ifExists = false;

					try {
						if (listMonth.size() > 0) {
							for (Statistic month : listMonth) {
								if (month.getType().equals("MONTH")) {
									if (month.getType().equals(type) && month.getTypeDetail().equals(HelpFunction.getMonth(dto.getMonth())) && month.getTypeSource().equals(typeSource)) {
										ifExists = true;
										month.setPosFacility(String.valueOf(Integer.parseInt(month.getPosFacility()) + posFacility));
										month.setPosTraining(String.valueOf(Integer.parseInt(month.getPosTraining()) + posTraining));
										month.setNegFacility(String.valueOf(Integer.parseInt(month.getNegTraining()) + negTraining));
										month.setNegTraining(String.valueOf(Integer.parseInt(month.getNegFacility()) + negFacility));
										break;
										}
								}else if (month.getType().equals("WEEK")) {
									if (month.getType().equals(type) && month.getTypeDetail().equals(dto.getDayOfWeek()) && month.getTypeSource().equals(typeSource)) {
										ifExists = true;
										month.setPosFacility(String.valueOf(Integer.parseInt(month.getPosFacility()) + posFacility));
										month.setPosTraining(String.valueOf(Integer.parseInt(month.getPosTraining()) + posTraining));
										month.setNegFacility(String.valueOf(Integer.parseInt(month.getNegTraining()) + negTraining));
										month.setNegTraining(String.valueOf(Integer.parseInt(month.getNegFacility()) + negFacility));
										break;
										}								}

							}
							
							if (!ifExists) {
								if (type.equals("MONTH")) {
									stat = new Statistic(type, HelpFunction.getMonth(dto.getMonth()), typeSource, String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
								}else if (type.equals("WEEK")) {
									stat = new Statistic(type, dto.getDayOfWeek(), typeSource, String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
								} 
								listMonth.add(stat); 
							}
						}
					}catch(java.util.ConcurrentModificationException e) {
						e.printStackTrace();
					}
										
					posTraining = 0;
					posFacility = 0;
					negFacility = 0;
					negTraining = 0;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		show(type, typeSource);
	}

	public void showRDD() {
		if (listMonthRdd == null) System.out.println("checkpoint");
		int i = 0;
			for (JavaRDD<DTO> rdd : listMonthRdd) {
				System.out.println(i + "/" + rdd.count());
//				if (rdd != null) {
//					RDDutils.show(rdd);
//				}
				i++;
			}
	}
	
	public void show(String type, String typeSource) {
		System.out.println("SHOWING RESULT LIST IN " + type + "/ " + typeSource);
		if (listMonth.size() == 0) System.out.println("EMPTY RESULT IN " + type + "/ " + typeSource); 
		for(Statistic month : listMonth) {
			System.out.println(month.getTypeDetail() + " : Training: " + month.getPosTraining() + "-" + month.getNegTraining());
			System.out.println(month.getTypeDetail() + " : Facility: " + month.getPosFacility() + "-" + month.getNegFacility());
			System.out.println("===========================");
		}
	}
	
	public void updateToDB(MongoClient mongoClient) {
		ConnectMongoDB mongoDB = new ConnectMongoDB(mongoClient);
		
		DB db = mongoClient.getDB(ConfigReader.readConfig("local.db.database"));
		
		DBCollection collection = db.getCollection(ConfigReader.readConfig("local.db.collection"));
		
		for (Statistic month : listMonth) {
			mongoDB.addOrUpdateNewDocument(month);
		}
		
	}
}