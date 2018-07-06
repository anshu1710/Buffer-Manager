package bufmgr;

import java.util.*;

import global.PageId;


public class HashTable {

	
	private final int HASH_SIZE = 103;
	private final int a = 13;
	private final int b = 17;
	private List<List<PageFramePair>> buckets;
	
	public String toString()
	{  
		String res="";
      for(List<PageFramePair>  l:buckets)
      { if(l==null)
    	  continue;
    	for(PageFramePair x : l)
    	{
    		if(x!=null)
    		res = res +"PageId: " + x.pageId+ " Frame:" + x.frameNum+ "\n";
    	}
      }
	return res;
	}
	
	private int hashFunction(int p)
	{
		return (a*p+b)%HASH_SIZE;
	}
	
	public HashTable() {
		buckets = new ArrayList<List<PageFramePair>>(HASH_SIZE);
		for(int i = 0; i< HASH_SIZE; i++)
		{
			buckets.add(i,null);
		}
	}
	
	public void put(int p,int frameNum)
	{
		int bucketNo = hashFunction(p);
		List<PageFramePair> bucket =  buckets.get(bucketNo);
		if(bucket==null)
		{
			bucket = new ArrayList<PageFramePair>();
			buckets.set(bucketNo,bucket);
		}
		bucket.add(new PageFramePair(p, frameNum));	
		
	}
	
	public Integer get(int p)
	{
		int bucketNo = hashFunction(p);
		List<PageFramePair> bucket =  buckets.get(bucketNo);
		
		if(bucket == null || bucket.isEmpty())
			return null;
		for(PageFramePair pf:bucket)
		{
			if(p == pf.pageId)
				return pf.frameNum;
		}
		return null;
	}
	
	/*
	 * Returns frame number if successful corresponding to page else -1 
	 * */
	public Integer remove(int p)
	{
		PageFramePair toRemove = null;
		int bucket = hashFunction(p);
		List<PageFramePair> l =  buckets.get(bucket);
		if(l == null || l.isEmpty())
			return null;
		
		for(PageFramePair pf:l)
		{
			if(p == pf.pageId){
				toRemove = pf;
				break;
			}
		}
		// Avoid ConcurrentAccessException
		if(toRemove != null)
		{
			l.remove(toRemove);
			return toRemove.frameNum;
		}
		return null;
		
	}
	
}