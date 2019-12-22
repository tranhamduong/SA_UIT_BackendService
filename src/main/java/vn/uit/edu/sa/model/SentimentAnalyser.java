package vn.uit.edu.sa.model;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.indexing.NDArrayIndex;

import vn.uit.edu.sa.util.ConfigReader;


public class SentimentAnalyser implements Serializable {
	private static SentimentIterator train;
	private static SentimentIterator test;
	private static MultiLayerNetwork net;
    
    private static final int batchSize = 50; //Number of examples in each minibatch
    private static final int vectorSize = 300; //Size of the word vectors. 300 in the model.
    private static final int nEpochs = 5; //Number of epochs (full passes of training data) to train on
    private static final int truncateReviewsToLength = 300; ////Truncate reviews with length (# words) greater than this
    
    private String DATA_PATH;
    private String WORD_VECTORS_PATH;
    
    public SentimentAnalyser(boolean isIntrainingMode) throws IOException {
        //DataSetIterators for training and testing respectively
        //Using AsyncDataSetItersentimentIteratorator to do data loading in a separate thread; this may improve performance vs. waiting for data to load
    	
    	DATA_PATH = System.getProperty("user.dir") + ConfigReader.readConfig("sa.model.dataPath.training");
    	WORD_VECTORS_PATH = System.getProperty("user.dir") + ConfigReader.readConfig("sa.model.word2vec");
    	
        WordVectors wordVectors = WordVectorSerializer.readWord2VecModel(new File(WORD_VECTORS_PATH));
        test = new SentimentIterator(DATA_PATH,wordVectors,batchSize,truncateReviewsToLength,false);

    	if (isIntrainingMode)
            train = new SentimentIterator(DATA_PATH,wordVectors,batchSize,truncateReviewsToLength,true);
    }
    
    public double[] testSample(String input) throws IOException{
    	
        //After training: load a single example and generate predictions
    	
		MultiLayerNetwork net = ModelSerializer.restoreMultiLayerNetwork(new File(System.getProperty("user.dir") + ConfigReader.readConfig("sa.model.aspect")));			
			
        INDArray features = test.loadFeaturesFromString(input, truncateReviewsToLength);
        INDArray networkOutput = net.output(features);
        long timeSeriesLength = networkOutput.size(2);
        INDArray probabilitiesAtLastWord = networkOutput.get(NDArrayIndex.point(0), NDArrayIndex.all(), NDArrayIndex.point((int) (timeSeriesLength - 1)));
        
        // if it is training
        if (probabilitiesAtLastWord.getDouble(0) > probabilitiesAtLastWord.getDouble(1)) {
        	
        	//System.out.println("Input: \n" + input);
	        //System.out.println("p(training): " + probabilitiesAtLastWord.getDouble(0));
	        //System.out.println("p(facility): " + probabilitiesAtLastWord.getDouble(1));	

        	MultiLayerNetwork net_training = ModelSerializer.restoreMultiLayerNetwork(new File(System.getProperty("user.dir") + ConfigReader.readConfig("sa.model.training")));			
			
	        INDArray features_training = test.loadFeaturesFromString(input, truncateReviewsToLength);
	        INDArray networkOutput_training = net_training.output(features_training);
	        long timeSeriesLength_training = networkOutput_training.size(2);
	        INDArray probabilitiesAtLastWord_training = networkOutput_training.get(NDArrayIndex.point(0), NDArrayIndex.all(), NDArrayIndex.point((int) (timeSeriesLength_training - 1)));
	        
	        //System.out.println("Input: \n" + input);
	        //System.out.println("p(positive): " + probabilitiesAtLastWord_training.getDouble(0));
	        //System.out.println("p(negative): " + probabilitiesAtLastWord_training.getDouble(1));	

	        double[] result = {probabilitiesAtLastWord.getDouble(0), probabilitiesAtLastWord.getDouble(1), probabilitiesAtLastWord_training.getDouble(0), probabilitiesAtLastWord_training.getDouble(1)};
	        
	        return result;
        } 
        
    	// if it is facility   	
    	//System.out.println("Input: \n" + input);
        //System.out.println("p(training): " + probabilitiesAtLastWord.getDouble(0));
        //System.out.println("p(facility): " + probabilitiesAtLastWord.getDouble(1));	
    	
    	MultiLayerNetwork net_facility = ModelSerializer.restoreMultiLayerNetwork(new File(System.getProperty("user.dir") + ConfigReader.readConfig("sa.model.facility")));			
		
        INDArray features_facility = test.loadFeaturesFromString(input, truncateReviewsToLength);
        INDArray networkOutput_facility = net_facility.output(features_facility);
        long timeSeriesLength_facility = networkOutput_facility.size(2);
        INDArray probabilitiesAtLastWord_facility = networkOutput_facility.get(NDArrayIndex.point(0), NDArrayIndex.all(), NDArrayIndex.point((int) (timeSeriesLength_facility - 1)));
    
        //System.out.println("Input: \n" + input);
        //System.out.println("p(positive): " + probabilitiesAtLastWord_facility.getDouble(0));
        //System.out.println("p(negative): " + probabilitiesAtLastWord_facility.getDouble(1));
        
        double[] result = {probabilitiesAtLastWord.getDouble(0), probabilitiesAtLastWord.getDouble(1), probabilitiesAtLastWord_facility.getDouble(0), probabilitiesAtLastWord_facility.getDouble(1)};
        
        return result;
    }
}
