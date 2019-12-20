package vn.uit.edu.sa.connectDB;


import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;

import vn.uit.edu.sa.dto.Statistic;
import vn.uit.edu.sa.spark.SparkConfigure;
import vn.uit.edu.sa.util.ConfigReader;
import vn.vitk.util.SparkContextFactory;

public class MongoSparkHelper<T> {
	
	private JavaSparkContext sparkContext;

	public MongoSparkHelper() {
		
	}
	
	public MongoSparkHelper(JavaSparkContext _sparkContext) {
		this.sparkContext = _sparkContext;
	}
	
	public static String getRemoteURI() {
		return "mongodb://" + ConfigReader.readConfig("uitsl.db.username") + ":"
				   + ConfigReader.readConfig("uitsl.db.password") + "@"
				   + ConfigReader.readConfig("uitsl.db.url") + ":"
				   + ConfigReader.readConfig("uitsl.db.port") + "/"
				   + ConfigReader.readConfig("uitsl.db.database") + "."
				   + ConfigReader.readConfig("uitsl.db.collection") + "?authSource="
				   + ConfigReader.readConfig("uitsl.db.authSource");
	}
	
	public static String getLocalURI() {
		return "mongodb://" + ConfigReader.readConfig("local.db.url") + "/"
				  + ConfigReader.readConfig("local.db.database") + "."
				  + ConfigReader.readConfig("local.db.collection");
	}
	
	public MongoSparkHelper(SparkConfigure sparkConfig, boolean ifRemote, String collectionName) {
		String uri = null;
		if (ifRemote) {
			uri = "mongodb://" + ConfigReader.readConfig("uitsl.db.username") + ":"
										   + ConfigReader.readConfig("uitsl.db.password") + "@"
										   + ConfigReader.readConfig("uitsl.db.url") + ":"
										   + ConfigReader.readConfig("uitsl.db.port") + "/"
										   + ConfigReader.readConfig("uitsl.db.database") + "."
										   //+ ConfigReader.readConfig("uitsl.db.collection") +"."
										   + collectionName + "?authSource="
										   + ConfigReader.readConfig("uitsl.db.authSource");
			
		}else {
			uri = "mongodb://" + ConfigReader.readConfig("local.db.url") + "/"
									  + ConfigReader.readConfig("local.db.database") + "."
									  + ConfigReader.readConfig("local.db.collection");
		}
		if (uri!= null)
			this.sparkContext = SparkContextFactory.create(uri);
		System.out.println(uri);
	}
	
	public JavaSparkContext getSparkContext() {
		return this.sparkContext;
	}
	
	public Dataset<T> read (String collectionName, Class<T> entityClass){
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", collectionName);
		readOverrides.put("readPreference.name", "secondaryPreferred");		
		ReadConfig readConfig = ReadConfig.create(sparkContext).withOptions(readOverrides);
		return MongoSpark.load(sparkContext, readConfig).toDS(entityClass);
	}
	
	public DataFrame read(String collectionName){
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", collectionName);
		readOverrides.put("readPreference.name", "secondaryPreferred");

		ReadConfig readConfig = ReadConfig.create(sparkContext).withOptions(readOverrides);
        return MongoSpark.load(sparkContext, readConfig).toDF();
	}
	
	public Dataset<T> read(String collectionName,List<Document> filterPipeline, Class<T> entityClass){
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", collectionName);
		readOverrides.put("readPreference.name", "secondaryPreferred");

        ReadConfig readConfig = ReadConfig.create(sparkContext).withOptions(readOverrides);
        
        return MongoSpark.load(sparkContext, readConfig).withPipeline(filterPipeline).toDS(entityClass);
	}

	public boolean write(String collectionName, List<Statistic> listMonth) {
		SparkConf sc = new SparkConf()
		        .setMaster("local")
		        .setAppName("MongoSparkConnector")
		        .set("spark.mongodb.input.uri", "mongodb://"+ ConfigReader.readConfig("local.db.url") + "/"
		        											+ ConfigReader.readConfig("local.db.database") + "."
		        											+ ConfigReader.readConfig("statistic"))
		        .set("spark.mongodb.output.uri", "mongodb://"+ ConfigReader.readConfig("local.db.url") + "/"
						+ ConfigReader.readConfig("local.db.database") + "."
						+ ConfigReader.readConfig("statistic"));
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		Map<String, String> writeOverrides = new HashMap<String, String>();
		writeOverrides.put("collection", collectionName);
		writeOverrides.put("writeConcern.w", "majority");
		WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
		
		JavaRDD<Document> documents = jsc.parallelize(listMonth).map(new Function<Statistic, Document>() {

			@Override
			public Document call(Statistic v1) throws Exception {
				Map<String, Object> documentMap = new HashMap<String, Object>();
				documentMap.put("type", v1.getType());
				documentMap.put("typeDetail", v1.getTypeDetail());
				documentMap.put("posTraining", v1.getPosTraining());
				documentMap.put("typeSource", v1.getTypeSource());
				documentMap.put("negTraining", v1.getNegTraining());
				documentMap.put("posFacility", v1.getPosFacility());
				documentMap.put("negFacility", v1.getNegFacility());

				Document doc = new Document(documentMap);
				return doc;
			}
		});		

		MongoSpark.save(documents, writeConfig);
		
		return true;
	}
}
