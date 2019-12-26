package vn.uit.edu.sa.connectDB;

import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.QueryBuilder;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.sun.tools.javac.code.Attribute.Array;

import vn.uit.edu.sa.dto.Statistic;
import vn.uit.edu.sa.util.ConfigReader;

public class ConnectMongoDB {
	
	private MongoClient mongoClient;
	
	public MongoClient ConnectMongoDB() {
		return new MongoClient();
	}
	
	public ConnectMongoDB(MongoClient _mongoClient) {
		this.mongoClient = _mongoClient;
	}
	
	public static MongoClient ConnectMongoDB(String connectionURI) {
		return new MongoClient(new MongoClientURI(connectionURI));
	}
	
	public static MongoClient ConnectMongoDB(String host, int port) {
		return new MongoClient(host, port);
	}
	
	public boolean checkIfDocumentExists(Statistic statistic) {
		
		MongoDatabase database = mongoClient.getDatabase(ConfigReader.readConfig("local.db.database"));
		
        MongoCollection<Document> collection = database.getCollection(ConfigReader.readConfig("local.db.collection"));
		
		String type = statistic.getType();
		String typeDetail = statistic.getTypeDetail();
		String typeSource = statistic.getTypeSource();
		
        Document query = new Document();
        query.append("type", type);
        query.append("typeDetail", typeDetail);
        query.append("typeSource", typeSource);
                
        Document result =  collection.find(query).first();        
        
        if (result == null)
        	return false;
        
        return true;
	}
	
	public List<Statistic> getData(){
		MongoDatabase database = mongoClient.getDatabase(ConfigReader.readConfig("local.db.database"));
		
        MongoCollection<Document> collection = database.getCollection(ConfigReader.readConfig("local.db.collection"));
        
        List<Statistic> list = new ArrayList<>();
        
        Block<Document> setListBlock = new Block<Document>() {

			@Override
			public void apply(Document t) {
				list.add(new Statistic(t.getString("type"), t.getString("typeDef"), t.getString("typeSource"), t.getString("posTraining"), t.getString("negTraining"), t.getString("posFacility"), t.getString("negFacility")));
			}
        	
        };
        collection.find().forEach(setListBlock);;
        
		return list;
	}
	
	public void addOrUpdateNewDocument(Statistic statistic) {
		MongoDatabase database = mongoClient.getDatabase(ConfigReader.readConfig("local.db.database"));
		
        MongoCollection<Document> collection = database.getCollection(ConfigReader.readConfig("local.db.collection"));
		
		Document doc = new Document("type", statistic.getType() )
				.append("typeDetail", statistic.getTypeDetail())
				.append("typeSource", statistic.getTypeSource())
				.append("posTraining", statistic.getPosTraining())
				.append("negTraining", statistic.getNegTraining())
				.append("posFacility", statistic.getPosFacility())
				.append("negFacility", statistic.getNegFacility());
        
//		Document doc = new Document("type", "MONTH")
//							.append("typeDetail", "FEB")
//								.append("typeSource", "POST")
//								.append("posTraining", "900")
//								.append("negTraining", "800")
//								.append("posFacility", "700")
//								.append("negFacility", "600");
		
		System.out.println(checkIfDocumentExists(statistic));
		if (!checkIfDocumentExists(statistic)) {
			collection.insertOne(doc);			
		}else{
		    Document tempUpdateOp = new Document("$set", doc);
			//collection.updateOne(Filters.and(Filters.eq("type", "MONTH"), Filters.eq("typeDetail", "FEB"), Filters.eq("typeSource", "POST")), tempUpdateOp);
			collection.updateOne(Filters.and(Filters.eq("type", statistic.getType()), Filters.eq("typeDetail", statistic.getTypeDetail()), Filters.eq("typeSource", statistic.getTypeSource())), tempUpdateOp);
		}
		
		System.out.println("add or update " + statistic.toString());
	}
	
	@SuppressWarnings("deprecation")
	public MongoClient connectMongoDBwithSecurity(String host, int port, String userName, String database, String password) throws UnknownHostException{
		MongoCredential mongoCredential = MongoCredential.createMongoCRCredential(userName, database, password.toCharArray());
		
		MongoClient _mongoClient = new MongoClient(new ServerAddress(host, port), Arrays.asList(mongoCredential));
		
		return _mongoClient;
	}
	
	public static Boolean closeConnection(JavaSparkContext jSparkContext) {
		try {
			jSparkContext.close();
			return true;
		}catch (Exception e) {
			System.out.println(e);
			return false;
		}
	}
}
