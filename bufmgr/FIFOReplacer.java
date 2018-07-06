package bufmgr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import chainexception.ChainException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;

public class FIFOReplacer implements Replacer{

    private BufMgr bufMgr;
    List<Integer> candidates= new ArrayList<>();
    List<BufDescr> listOfFrames = null;

    public FIFOReplacer(BufMgr bufMgr)
    {
        this.bufMgr = bufMgr;
        listOfFrames = bufMgr.getBufDescList();
    }
    public Integer getEmptyFrame() throws ChainException
    {
    candidates= new ArrayList<>();

    listOfFrames = bufMgr.getBufDescList();
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
       
    double min_fifo = Double.MAX_VALUE;
    Integer toEmptyIndex = -1;
   
    for(Integer i:candidates)
    {  
        BufDescr frame = listOfFrames.get(i);
        if(frame== null)
         return i;
        double  mintime = frame.getAccessTimes().get(0);
        if(mintime < min_fifo)
        {
            mintime = min_fifo;
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


}