package sentiment;

import java.net.UnknownHostException;
import java.io.*;
import java.util.*;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

// list nodes store sentiment keywords from text files
class Sentiment {
	String word;
	boolean positivity;
	Sentiment next;
	
	// constructs sentiment object
	public Sentiment (String word, boolean positivity, Sentiment next) {
		this.word = word;
		this.positivity = positivity;
		this.next = next;
	}
}

public class sentimentAnalyzer {
	// calculates hashtable index of a given word
	public static int hashFunction(String word) {
		int sum, index;
		char character;
		
		index = sum = 0;
		for (int i = 0; i < word.length(); i++) {
			sum = (int) word.charAt(i);
		}
		index = sum%1000;		
		return index;
	}
	
	public static void main(String[] args) 
			throws UnknownHostException, FileNotFoundException {
		
		// variable declarations and initialization
		int index, sentCount, count;
		String pword, nword, word, number;

		index = count = sentCount = 0;
		
		pword = "";
		nword = "";
		word = "";
		number = "";
		
		// SET UP HASHTABLE OF SENTIMENT WORDS FROM TXT FILES
		
		// declare hashtable and initialize
		Sentiment[] hashtable = new Sentiment[1000];
		for (int i = 0; i < 1000; i++) {
			hashtable[i] = null;
		}
		
		// retrieve positive words and insert into hashtable
		Scanner sc1 = new Scanner(new File("positive_words.txt"));
		while (sc1.hasNext()) {
			pword = sc1.next();
			index = hashFunction(pword);	
			hashtable[index] = new Sentiment(pword, true, hashtable[index]);
		}	
		// retrieve negative words and insert into hashtable
		Scanner sc2 = new Scanner(new File("negative_words.txt"));
		while (sc2.hasNext()) {
			nword = sc2.next();
			index = hashFunction(nword);	
			hashtable[index] = new Sentiment(nword, false, hashtable[index]);
		}	
		
		// GET DATABASE CONNECTION
		
		// get new connection to database "cs336" (should be running)
		DB db = (new MongoClient("localhost", 27017)).getDB("cs336");

		// retrieve collection object
		DBCollection split = db.getCollection("unlabel_review_after_splitting");
		DBCollection unsplit = db.getCollection("unlabel_review");

		// CATEGORIZE REVIEWS
		
		// iterate through documents
		DBCursor cursor = split.find();
		try {
			while (cursor.hasNext()) {
			
				// get review from document
				BasicDBObject object = (BasicDBObject) cursor.next();				
				String review = (String) object.get("review").toString();
				//System.out.println("Split Review: " + object.toString());
				
				// get associated review ID
				String reviewID = (String) object.get("id").toString();
				//System.out.println("ReviewID: " + reviewID);

				/*
				BasicDBObject query = new BasicDBObject();
		        query.put("id", reviewID);
		        DBCursor c = unsplit.find(query);
		        while (c.hasNext()) {
		            System.out.println("Unsplit Review: " + c.next());
		        }
				*/
				
				// If I knew how to extract subfields I would have put that code here
				StringTokenizer tok = new StringTokenizer(review, "[]{}:,\" ");
				
				boolean flag = false;
				
				// iterate through tokens in review
				while (tok.hasMoreTokens()) {
					String token = tok.nextToken();

					// extract count
					if (token.equals("count")) {
						number = tok.nextToken();
						count = Integer.parseInt(number);
						//System.out.println("count: " + count);
					}
					// extract word
					if (token.equals("word")) {
						word = tok.nextToken();
						flag = true;
						//System.out.println("word: " + word);			
					}
					
					// IDENTIFY WORD AS POSITIVE OR NEGATIVE OR NEITHER
					
					if (flag) {
						// locate word in table
						int idx = hashFunction(word);
						Sentiment ptr = hashtable[idx];

						// iterate through linked list
						while (ptr != null) {
							// match found - identify sentiment
							if (ptr.word.equals(word)) {
								if (ptr.positivity) {
									//System.out.println(word + " (positive + " + count + ")");
									sentCount = sentCount + count;
									flag = false;
									break;
								} else {
									//System.out.println(word + " (negative + " + count + ")");
									sentCount = sentCount - count;
									flag = false;
									break;
								}
							} else {
								ptr = ptr.next;
							}
						}
						// if ptr == null, word could not be found in hashtable
						// not used in review's sentiment analysis
					}
				} // end loop through the review
				
				// ANALYZE SENTIMENT COUNT
				
				//System.out.println("Sentiment Count: " + sentCount);
				if (sentCount >= 0) {
					//System.out.println("This review is positive");
					unsplit.update(new BasicDBObject("id", reviewID), new BasicDBObject("$set", new BasicDBObject("sentiment", "positive"))); 
				} else {
					//System.out.println("This review is negative");
					unsplit.update(new BasicDBObject("id", reviewID), new BasicDBObject("$set", new BasicDBObject("sentiment", "negative"))); 
				}
				
				sentCount = 0; // reset	
			} // end loop through documents (reviews)
			
		} finally {
			cursor.close();
		}

		// output labeled unsplit reviews
		DBCursor c = unsplit.find();
		try {
		   while(c.hasNext()) {
		       System.out.println(c.next());
		   }
		} finally {
		   c.close();
		}

	}
}