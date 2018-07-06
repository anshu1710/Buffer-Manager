package bufmgr;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import chainexception.ChainException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.PageId;

public class LRFUReplacer implements Replacer{
	
	private BufMgr bufMgr;

	public LRFUReplacer(BufMgr bufMgr){
		this.bufMgr = bufMgr;
	}
	
	public Integer getEmptyFrame() throws ChainException
	{
		
		List<Integer> candidates= new ArrayList<>();

		List<BufDescr> listOfFrames = bufMgr.getBufDescList();
		for(int i = 0;i<listOfFrames.size();i++)
		{
			BufDescr frame = listOfFrames.get(i);
			
			if(frame == null || frame.getPin_count()==0)
				candidates.add(i);
		}
		if(candidates.isEmpty())
		{
			throw new BufferPoolExceededException(null,"BUFMGR: No empty frames in buffer");
		}
		
		double min_crf = Double.MAX_VALUE;
		Integer toEmptyIndex = -1;
        
		for(Integer i:candidates)
		{   BufDescr frame = listOfFrames.get(i);
			double  crf = getCRFValue(frame);
			if(crf < min_crf)
			{
				min_crf = crf;
				toEmptyIndex = i;
			}
		}
     
		BufDescr toEmpty = listOfFrames.get(toEmptyIndex);
		if(toEmpty != null && toEmpty.isDirty())
			bufMgr.flushPage(toEmpty.getPageId());
		
		
		if(toEmpty != null )
		{
		bufMgr.getHashTable().remove(toEmpty.getPageId().pid);
		toEmpty.reset();
		}
		
		return toEmptyIndex;
	}
	
	private Double getCRFValue(BufDescr f)
	{
		 if(f==null)
		      return Double.MIN_VALUE;
	    List<Long> times = f.getAccessTimes();
	   
		Long currTime =  Instant.now().toEpochMilli();
		Double crf = 0.0;
		
		for(Long time : times)
		{
			crf += fun(currTime-time);
		}
		return crf;
	}
	
   private Double fun(Long t)
   {
	   return  (1.0/(t+1));
   }
}
