package vn.uit.edu.sa.backEndService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import vn.uit.edu.sa.dto.DTO;
import vn.uit.edu.sa.dto.Statistic;
import vn.uit.edu.sa.languagePreprocessor.LanguagePreprocessor;
import vn.uit.edu.sa.model.SentimentAnalyser;
import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.HelpFunction;

public class StatisticCalculator  implements Serializable{
	
	private Statistic stat;
	
	private double[] result;
	
	List<Statistic> listMonth;
	
	JavaRDD<DTO> monthRDD;
	
	List<JavaRDD<DTO>> listMonthRdd;
	
	List<JavaRDD<String>> listMonthStringRdd;

	int posTraining = 0;
	int negTraining = 0;
	int posFacility = 0;
	int negFacility = 0;
	
	public StatisticCalculator() {
		
		listMonth = new ArrayList<Statistic>();
		
		listMonthRdd = new ArrayList<JavaRDD<DTO>>();
		
		monthRDD = null;
	}
	
	public void doSentimentAnalystWithAlreadyData(SparkConfigure spark, JavaRDD<DTO> rdd, String type, String typeSource, List<Statistic> list) {
		listMonth = list;
		doSentimentAnalyst(spark, rdd, type, typeSource);
	}
	
	
	public void doSentimentAnalyst(SparkConfigure spark, JavaRDD<DTO> rdd, String type, String typeSource) {
		try {
			SentimentAnalyser model = new SentimentAnalyser(false);
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
					
					for (Statistic month : listMonth) {
						if (month.getType().equals(type) && month.getTypeDetail().equals(HelpFunction.getMonth(dto.getMonth())) && month.getTypeSource().equals(typeSource)) {
							month.setPosFacility(month.getPosFacility() + posFacility);
							month.setPosTraining(month.getPosTraining() + posTraining);
							month.setNegFacility(month.getNegFacility() + negFacility);
							month.setNegTraining(month.getNegTraining() + negTraining);
						}else {
							stat = new Statistic(type, HelpFunction.getMonth(dto.getMonth()), typeSource, String.valueOf(posTraining), String.valueOf(negTraining), String.valueOf(posFacility), String.valueOf(negFacility));
							listMonth.add(stat);
						}
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
	
	public void show() {
		for(Statistic month : listMonth) {
			System.out.println(month.getTypeDetail() + " : Training: " + month.getPosTraining() + "-" + month.getNegTraining());
			System.out.println(month.getTypeDetail() + " : Facility: " + month.getPosFacility() + "-" + month.getNegFacility());
			System.out.println("===========================");
		}
	}
}
