package edu.cmu.courses.simplemr.mapreduce;

import java.util.*;

/**
 * The Output Collector is a input of user applications.
 * User add a entry to the result by calling collect().
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class OutputCollector {
	
    private PriorityQueue<Pair<String, String>> collection;
    private Set<String> keys;

    public OutputCollector() {
    	//collection = new PriorityQueue<Pair<String, String>>(10, new Comparator<Pair<String, String>>() { //ibrahim: change queue initial capacity
    	//collection = new PriorityQueue<Pair<String, String>>(1000, new Comparator<Pair<String, String>>() {
        collection = new PriorityQueue<Pair<String, String>>(1000000, new Comparator<Pair<String, String>>() {
            public int compare(Pair<String, String> o1, Pair<String, String> o2) {
                if(o1.getKey().compareTo(o2.getKey()) == 0)
                    return o1.getValue().compareTo(o2.getValue());
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        keys = new TreeSet<String>();
    }

    public void collect(String key, String value){
        collection.add(new Pair<String, String>(key, value));
        keys.add(key);
    }

    public Iterator<Pair<String, String>> getIterator(){
        return collection.iterator();
    }

    public int getKeyCount(){
        return keys.size();
    }

    public TreeMap<String, List<String>> getMap(){
        TreeMap<String, List<String>> map = new TreeMap<String, List<String>>();
        for(Pair<String, String> pair : collection){
            if(map.containsKey(pair.getKey())){
                map.get(pair.getKey()).add(pair.getValue());
            } else {
                List<String> values = new ArrayList<String>();
                values.add(pair.getValue());
                map.put(pair.getKey(), values);
            }
        }
        return map;
    }
    
    public HashMap<String, List<Integer>> getMapOutput(int combinerFlag) {
    	
        HashMap<String, List<Integer>> map = new HashMap<String, List<Integer>>();
        List<Integer> previousValue = null;
        Integer newValue = null;
        int pairValue = 0;
        
        for(Pair<String, String> pair : collection){
        	
        	pairValue = Integer.parseInt(pair.getValue());
        	
            if(map.containsKey(pair.getKey())){
            	
            	previousValue = map.get(pair.getKey());
            	
                switch (combinerFlag) {
                	case 1: // Sum
                		newValue = new Integer(previousValue.get(0).intValue()+pairValue);
                		previousValue.set(0, newValue);
                		break;
                	case 2: // Max
                		
                		if (pairValue>previousValue.get(0).intValue())
                			previousValue.set(0, new Integer(pairValue));
                		break;
                	case 3: // Min
                		
                		if (pairValue<previousValue.get(0).intValue())
                			previousValue.set(0, new Integer(pairValue));
                		break;
                	default:
                		previousValue.add(pairValue);
                		break;
                }
                
            } else {
            	
            	previousValue = new ArrayList<Integer>();
            	previousValue.add(pairValue);
                map.put(pair.getKey(), previousValue);
            }
        }
        return map;
    }
}
