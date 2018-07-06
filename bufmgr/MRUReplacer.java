package bufmgr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import chainexception.ChainException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;

public class MRUReplacer implements Replacer{

    private BufMgr bufMgr;
    List<Integer> candidates= new ArrayList<>();
    List<BufDescr> listOfFrames = null;

   
    public MRUReplacer(BufMgr bufMgr)
    {
        this.bufMgr = bufMgr;
        listOfFrames = bufMgr.getBufDescList();
    }
   
    public Integer getEmptyFrame() throws  ChainException
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

   
    double max_mru = Double.MIN_VALUE;
    Integer toEmptyIndex = -1;
   
    for(Integer i:candidates)
    {  
        BufDescr frame = listOfFrames.get(i);
        if(frame== null)
            return i;
        int size = frame.getAccessTimes().size();
        double  maxtime = frame.getAccessTimes().get(size-1);
        if(maxtime > max_mru)
        {
            maxtime = max_mru;
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