package vn.uit.edu.sa.languagePreprocessor;

import java.io.File;

import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.LineSentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import vn.uit.edu.sa.util.ConfigReader;

public class Vectorize implements java.io.Serializable{
	
	private SentenceIterator iter = null;

	public Vectorize() {
		iter = new LineSentenceIterator(new File(System.getProperty("user.dir") + ConfigReader.readConfig("dir.pre.finish"))); 
	}
	
	public Vectorize(String filePath) {
		iter = new LineSentenceIterator(new File(filePath));
	}
	
	public void run() {
		try {
	        TokenizerFactory t = new DefaultTokenizerFactory();
	        t.setTokenPreProcessor(new CommonPreprocessor());
	        
	        Word2Vec vec = new Word2Vec.Builder()
	                .minWordFrequency(5)
	                .layerSize(300)
	                .seed(42)
	                .windowSize(5)
	                .iterate(iter)
	                .tokenizerFactory(t)
	                .build();
	        
	        vec.fit();

	        WordVectorSerializer.writeWordVectors(vec, System.getProperty("user.dir") + ConfigReader.readConfig("sa.model.word2vec"));

	        System.out.println("Finish vectorized!");
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
