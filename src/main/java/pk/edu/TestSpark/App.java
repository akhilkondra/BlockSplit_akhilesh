package pk.edu.TestSpark;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.util.List;

import scala.Tuple2;

public class App {
	public static int sum = 0;
	public static int percentagecal;
	

	public static void main(String[] args) throws IOException {
		String logFile1 = System.getProperty("user.dir") + "//Testing.csv";
	

		SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("MyApp");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logdata1 = sc.textFile(logFile1).cache();
		
	        
		JavaRDD<String> lines1 = logdata1.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split("\n"));
			}
		});

		

		JavaPairRDD<String, String> wordToCountMap1 = lines1.flatMapToPair(new PairFlatMapFunction<String, String, String>(){
			
			public List<Tuple2<String, String>> call(String s) throws Exception {
				List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
				TaskContext task =TaskContext.get();
				System.out.println("task Partition id"+task.partitionId());
				String key[] = s.split(",");
				for(int i=1;i<key.length;i++)
				{
				
				if(!key[i].isEmpty())
				{		
				results.add(new Tuple2<String, String>(key[i] + "." +task.partitionId(),"1"));
				results.add(new Tuple2<String, String>("Ignore_"+key[0],"Ignore_"+key[i]));
				}
				
				}
				return results;
			}
		});
		
		
		JavaPairRDD<String, String> wordCounts1 = wordToCountMap1
				.reduceByKey(new Function2<String, String, String>() {
				public String call(String first, String second) throws Exception {
				if(first.contains("Ignore_")||second.contains("Ignore_"))
				{
					return first +"," +second;
				}
				else
				{
					int f1 = Integer.parseInt(first);
					int s1 = Integer.parseInt(second);
					int sum = f1+s1;
					String rsum=Integer.toString(sum); 
					return rsum;
				}
				
				}
				});

		HashMap<String, String> hm = new HashMap<String, String>();
		final HashMap<String, String> culusterSignature = new HashMap<String, String>();
		
		final HashMap<String, Integer> entitykey = new HashMap<String, Integer>();
		final HashMap<String, Integer> entitykeycomp = new HashMap<String, Integer>();
		ArrayList<Integer> cmp = new ArrayList<Integer>();

		List<Tuple2<String, String>> output1 = wordCounts1.collect();
		for (Tuple2<?, ?> tuple : output1) {
		if(tuple._1().toString().contains("Ignore_"))
		{
			culusterSignature.put(tuple._1().toString(), tuple._2().toString());
		}
		else
		{
		hm.put(tuple._1().toString(), tuple._2().toString());
		}
		System.out.println(tuple._1() + ": " + tuple._2());
		}
		
	

		for (Entry<String, String> clustersign : culusterSignature.entrySet()) {
		System.out.println(clustersign.getKey()+"Values "+clustersign.getValue());
		}
		
		int index = 0;
		System.out.println("****************************");
		System.out.println("* Block Distribution matrix*");
		System.out.println("****************************");
		System.out.println("------------------------------------------------------------");
		System.out.println("|Key\t|\t0\t|\t1\t|\tcombinations\t|");
		System.out.println("------------------------------------------------------------");
		for (Entry<String, String> entry : hm.entrySet()) {
			String key = entry.getKey().toString();
			String keyings[] = key.split("\\.");
			if (!entitykey.containsKey(keyings[0])) {

				entitykey.put(keyings[0], 0);
				System.out.print("|\t" + keyings[0]);
				String value1 = "";
				String value2 = "";
				if (hm.containsKey(keyings[0] + ".0")) {
					value1 = hm.get(keyings[0] + ".0");
				} else {
					value1 = "0";
				}
				if (hm.containsKey(keyings[0] + ".1")) {
					value2 = hm.get(keyings[0] + ".1");
				} else {
					value2 = "0";
				}

				int val1 = Integer.parseInt(value1);
				int val2 = Integer.parseInt(value2);
				int c1 = (val1 * (val1 - 1)) / 2;
				int c2 = (val2 * (val2 - 1)) / 2;
				int sum = c1 + c2;
				int Comparitions = (val1 * val2) + sum;

				cmp.add(Comparitions);
				entitykey.put(keyings[0], index);
				index = index + 1;
				entitykeycomp.put(keyings[0], Comparitions);
				System.out.println("\t|\t" + val1 + "\t|\t" + val2 + "\t|\t" + Comparitions + "\t|");
			}

		}

		System.out.println("------------------------------------------------------------");

		for (int i = 0; i < cmp.size(); i++) {
			sum = sum + cmp.get(i);
		}

		System.out.println(" TOTAL COMPARISION POSSIBLE  = " + sum);
		percentagecal = (int) (sum * 0.20);
		System.out.println(percentagecal);

		JavaRDD<String> logdata3 = sc.textFile(logFile1, 2).cache();
		

		JavaRDD<String> lines3 = logdata3.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split("\n"));
			}
		});

		
		for (Entry<String, String> clustersign : culusterSignature.entrySet()) {
			String[] results=clustersign.getValue().split(",");
			ArrayList<Integer> appends = new ArrayList<Integer>();
			for(int i=0;i<results.length;i++)
			{
//				if (!culusterKeys.containsKey(results[i])) {
//					culusterKeys.put(results[i], clusterIndex);
//					appends.add(clusterIndex);
//					clusterIndex=clusterIndex+1;
//				}
//				else
//				{
					String keym=results[i].substring(7);
					System.out.println(keym);
					appends.add(entitykey.get(keym));
					
//				}
				
				Collections.sort(appends);
				String finalAppends ="";
				for(int j=0;j<appends.size();j++)
				{
					if(j==0)
					{
						finalAppends=Integer.toString(appends.get(j));
					}
					else
					{
						finalAppends=finalAppends+"/"+Integer.toString(appends.get(j));
						
					}		
				}
				clustersign.setValue(finalAppends);
		
			}
			
			
		}
		
		JavaPairRDD<String, String> wordToCountMap3 = lines3
				.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

					public List<Tuple2<String, String>> call(String s) {
						List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
						String key[] = s.split(",");
						for(int i=1;i<key.length;i++)
						{
						if(!key[i].isEmpty())
						{
							System.out.println("Key is "+ key[i]);
							
						int blckindex = entitykey.get(key[i]);
						System.out.println("Block index"+blckindex);
						int empcp = entitykeycomp.get(key[i]);
						TaskContext task =TaskContext.get();
						if (percentagecal > empcp) {
							results.add(new Tuple2<String, String>("0." + blckindex + ".*", key[0]+"{"+culusterSignature.get("Ignore_"+key[0])+"}"));
						} else {
							
							results.add(new Tuple2<String, String>(task.partitionId()+"." + blckindex + "."+0, key[0]+"{"+culusterSignature.get("Ignore_"+key[0])+"}"));
							String x = key[0] + "hiphen"+task.partitionId();
							results.add(new Tuple2<String, String>("S." + blckindex + ".0X1", x+"{"+culusterSignature.get("Ignore_"+key[0])+"}"));
						}
						}
						}

						return results;
					}
				});
		
		wordToCountMap3.groupByKey();
		
		JavaPairRDD<String, String> wordCounts3 = wordToCountMap3.reduceByKey(
				new Function2<String, String, String>() {
					public String call(String first, String second) throws Exception {

							return first + "," + second;
						
					}
				});

		List<Tuple2<String, String>> output = wordCounts3.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + "[" + tuple._2() + "]");
			String token[]=tuple._1().toString().split("\\.");
			int finalTokenInt=Integer.parseInt(token[1]);
			
			String sp = tuple._2().toString();
			String[] dp = sp.split(",");
			for (int i = 0; i < dp.length; i++) {
				for (int j = (i + 1); j < dp.length; j++) {
					boolean eleminatePair=false;
					Pattern pattern1 = Pattern.compile("\\{.*.*\\}");
					Matcher matcher = pattern1.matcher(dp[i]);
					ArrayList<Integer> ji = new ArrayList<Integer>();
					while (matcher.find()) {
						
					String sim=matcher.group(0);
					sim=sim.replaceAll("\\{", "");
					sim=sim.replaceAll("\\}", "");
					sim=sim.trim();
					String r1[]=sim.split("/");
					
					
					for(int lp=0;lp<r1.length;lp++)
					{
						ji.add(Integer.parseInt(r1[lp]));
						
					}
					
					}
					
					pattern1 = Pattern.compile("\\{.*.*\\}");
					matcher = pattern1.matcher(dp[j]);
					ArrayList<Integer> pi = new ArrayList<Integer>();
					while (matcher.find()) {
						
						String sim=matcher.group(0);
						sim=sim.replaceAll("\\{", "");
						sim=sim.replaceAll("\\}", "");
						sim=sim.trim();
						String r1[]=sim.split("/");
						
						
						for(int lp=0;lp<r1.length;lp++)
						{
							pi.add(Integer.parseInt(r1[lp]));
							
						}
					}
					
		
					Set<Integer> s1 = new HashSet<Integer>(ji);
					Set<Integer> s2 = new HashSet<Integer>(pi);
					s1.retainAll(s2);
					Integer[] result = s1.toArray(new Integer[s1.size()]);
					
					if(result.length<1)
					{
						eleminatePair=false;	
					}
					else
					{
						if(result[0]==finalTokenInt)
						{
							eleminatePair=false;	
						}
						else if(result[0]!=finalTokenInt)
						{
							eleminatePair=true;
						}
						
					}
					
					if(eleminatePair==true)
					{
						String xb=dp[i].replaceAll("\\{.*.*\\}", "");
						String xc=dp[j].replaceAll("\\{.*.*\\}", "");
						System.out.println("Eleminated Pair ===> [" + xb + "," + xc + "]");
						continue;
					}
					else{
					
					if (((dp[i].contains("hiphen0")) && (dp[j].contains("hiphen0")))) {
					} 
					else if (((dp[i].contains("hiphen1")) && (dp[j].contains("hiphen1")))) {
					}
					else {
						String xb = dp[i].replaceAll("\\{.*.*\\}", "");;
						String xc = dp[j].replaceAll("\\{.*.*\\}", "");
						if (dp[i].contains("hiphen")) {
							xb = xb.substring(0, (xb.length() - 7));
						}
						if (dp[j].contains("hiphen")) {
							xc = xc.substring(0, (xc.length() - 7));
						}
						//System.out.println("[" + xb + "," + xc + "]");
						double similar= similarity(xb, xc);
						if(similar>=0.5)
						{
							System.out.println("=============================================================");
							System.out.println("[" + xb + "," + xc + "] has " +
									similar+ " Similarity");
							System.out.println("=============================================================");
							
						}
						else{
						System.out.println("[" + xb + "," + xc + "] has " +
								similar+ " Similarity");
						}
						
						
					}
					}
				}
			}
			
			
		}
		
		sc.close();
		
	}
	
	  public static double similarity(String s1, String s2) {
		    String longer = s1, shorter = s2;
		    if (s1.length() < s2.length()) { 
		    	// longer should always have greater length
		      longer = s2; shorter = s1;
		    }
		    int longerLength = longer.length();
		    if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
		    return (longerLength - editDistance(longer, shorter)) / (double) longerLength;

		  }
	  public static int editDistance(String s1, String s2) {
		    s1 = s1.toLowerCase();
		    s2 = s2.toLowerCase();

		    int[] costs = new int[s2.length() + 1];
		    for (int i = 0; i <= s1.length(); i++) {
		      int lastValue = i;
		      for (int j = 0; j <= s2.length(); j++) {
		        if (i == 0)
		          costs[j] = j;
		        else {
		          if (j > 0) {
		            int newValue = costs[j - 1];
		            if (s1.charAt(i - 1) != s2.charAt(j - 1))
		              newValue = Math.min(Math.min(newValue, lastValue),
		                  costs[j]) + 1;
		            costs[j - 1] = lastValue;
		            lastValue = newValue;
		          }
		        }
		      }
		      if (i > 0)
		        costs[s2.length()] = lastValue;
		    }
		    return costs[s2.length()];
		  }

}
