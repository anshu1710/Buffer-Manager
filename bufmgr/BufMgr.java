package bufmgr;
import java.io.IOException;
import java.util.*;

import chainexception.ChainException;
import diskmgr.DiskMgr;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

public class BufMgr {
	
	private byte[][] bufpool;
	private List<BufDescr> bufDescr;
	private HashTable directory;
	private int currFrame;
	private int timeCounter=1;
	private boolean bufPoolFull;
	private Replacer replacer;
	
/**
* Create the BufMgr object.
* Allocate pages (frames) for the buffer pool in main memory and
* make the buffer manage aware that the replacement policy is
* specified by replacerArg (e.g., LH, Clock, LRU, MRU, LRFU, etc.).
*
* @param numbufs number of buffers in the buffer pool
* @param lookAheadSize number of pages to be looked ahead, you can ignore that parameter
* @param replacementPolicy Name of the replacement policy, that parameter will be set to "LRFU"
*/
public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {
	
	bufpool = new byte[numbufs][];
	bufDescr = new ArrayList<BufDescr>(numbufs);
	for(int i=0; i< numbufs; i++)
	{
		bufDescr.add(i,null);
	}	
//	diskMgr = new DiskMgr();
	directory = new HashTable();
	currFrame = 0;
	bufPoolFull = false;
	
	replacer = new LRFUReplacer(this);
	
	if("FIFO".equals(replacementPolicy))
	{
		replacer = new FIFOReplacer(this);
	}
	if("MRU".equals(replacementPolicy))
	{
		replacer = new MRUReplacer(this);
	}
	
}
/**
* Pin a page.
* First check if this page is already in the buffer pool.
* If it is, increment the pin_count and return a pointer to this
* page.
* If the pin_count was 0 before the call, the page was a
* replacement candidate, but is no longer a candidate.
* If the page is not in the pool, choose a frame (from the
* set of replacement candidates) to hold this page, read the
* page (using the appropriate method from diskmgr package) and pin it.
* Also, must write out the old page in chosen frame if it is dirty
* before reading new page.__ (You can assume that emptyPage==false for
* this assignment.)
*
* @param pageno page number in the Minibase.
* @param page the pointer pointing to the page.
* @param emptyPage true (empty page); false (non-empty page)
 * @throws IOException 
*/
public void pinPage(PageId pageno, Page page, boolean emptyPage) throws ChainException {
	Integer frameNo = directory.get(pageno.pid);
	BufDescr frameDesc ;
	if(frameNo != null){
		frameDesc = bufDescr.get(frameNo);
		frameDesc.addAccessTime();
		frameDesc.incrPinCount();
		page.setpage(bufpool[frameNo]);
	}else{
		
		//diskMgr.allocate_page(pageno);
		if(!bufPoolFull){
			
			Page temp = new Page();
			
			frameDesc = new BufDescr();
			bufDescr.set(currFrame, frameDesc);
			
			try {
				Minibase.DiskManager.read_page(pageno,temp);
			} catch (IOException e) {
				throw new DiskMgrException(e,"DB.java: read_page() failed ");
			}
			
			bufpool[currFrame] = temp.getData();
			page.setpage(temp.getpage());
		//	System.out.println(bufpool[currFrame] +" =  " + page.getData());
			
		//	System.out.println(pageno +" :  "+ Convert.getIntValue(0, bufpool[currFrame]) + ":: " + Convert.getIntValue(0, page.getData()));
			
			directory.put(pageno.pid,currFrame);
			frameDesc.incrPinCount();
			frameDesc.setPageId(pageno.pid);
			frameDesc.addAccessTime();
			currFrame++;
			
			if(currFrame == bufpool.length){
				bufPoolFull = true;
			}
		}
		else{
			Integer emptyFrameNo = replacer.getEmptyFrame();
			
			Page temp = new Page();
			
			frameDesc = bufDescr.get(emptyFrameNo);
			if(frameDesc == null)
			{
				frameDesc = new BufDescr();
			    bufDescr.set(emptyFrameNo, frameDesc);	
			}
			
			bufpool[emptyFrameNo] = temp.getData();
			page.setpage(temp.getpage());
			
			try{
			Minibase.DiskManager.read_page(pageno,temp);
			}
			catch(IOException e){
				throw new DiskMgrException(e,"DB.java: read_page() failed ");
			}
			
			bufpool[emptyFrameNo] = temp.getData();
			directory.put(pageno.pid,emptyFrameNo);
			
			frameDesc.setPageId(pageno.pid);
			frameDesc.incrPinCount();
			frameDesc.addAccessTime();
		}
	}
}
/**
* Unpin a page specified by a pageId.
* This method should be called with dirty==true if the client has
* modified the page.
* If so, this call should set the dirty bit for this frame.
* Further, if pin_count>0, this method should decrement it.
*If pin_count=0 before this call, throw an exception to report error.
*(For testing purposes, we ask you to throw
* an exception named PageUnpinnedException in case of error.)
*
* @param pageno page number in the Minibase.
* @param dirty the dirty bit of the frame
*/


public void unpinPage(PageId pageno, boolean dirty) throws ChainException {
	 
	
	Integer frameNo = directory.get(pageno.pid);
	BufDescr frameDesc;
	if(frameNo != null){
		frameDesc = bufDescr.get(frameNo);
		if(frameDesc ==null)
		{
			System.out.println("pageNo:" + pageno);
		}
		frameDesc.decrPinCount();
		if(dirty==true)
			frameDesc.setDirtybit(true);
	}
	else
	{
		throw new HashEntryNotFoundException(null,"BUFMGR: Frame not in buffer pool");
	}
}

/**
* Allocate new pages.
* Call DB object to allocate a run of new pages and
* find a frame in the buffer pool for the first page
* and pin it. (This call allows a client of the Buffer Manager
* to allocate pages on disk.) If buffer is full, i.e., you
* can't find a frame for the first page, ask DB to deallocate
* all these pages, and return null.
*
* @param firstpage the address of the first page.
* @param howmany total number of allocated new pages.
*
* @return the first page id of the new pages.__ null, if error.
 * @throws ChainException 
 * @throws IOException 
*/

public PageId newPage(Page firstpage, int howmany) throws ChainException { 
		
		 PageId newPageId = new PageId();
		 try {
			Minibase.DiskManager.allocate_page(newPageId,howmany);
		} catch (IOException e1) {
			throw new DiskMgrException(e1,"DB.java: allocate_page() failed");
		}
		 try{
		 pinPage(newPageId,firstpage,false);
		 }
		 catch(BufferPoolExceededException e)
		 {
			 try{
			 Minibase.DiskManager.deallocate_page(newPageId);
			 }
			 catch (ChainException ce) {
					throw new DiskMgrException(ce,"DB.java: deallocate_page() failed");
				}
		newPageId = null;
		 }
		 return newPageId;
}
/**
* This method should be called to delete a page that is on disk.
* This routine must call the method in diskmgr package to
* deallocate the page.
*
* @param globalPageId the page number in the data base.
*/
public void freePage(PageId globalPageId) throws ChainException {
	Integer frameNo = directory.get(globalPageId.pid); 
	if(frameNo!= null)
	{ BufDescr f = bufDescr.get(frameNo);
		if(f.getPin_count() > 0)
		{
			throw new PagePinnedException(null,"BUFMGR: Page is pinned.Cannot free");
		}
		else
		{
			directory.remove(globalPageId.pid);
			f.reset();
			bufDescr.set(frameNo, null);
		}
	}
	try {
		Minibase.DiskManager.deallocate_page(globalPageId);
	} catch (ChainException e) {
		throw new DiskMgrException(e,"DB.java: deallocate_page() failed");
	}
}
/**
* Used to flush a particular page of the buffer pool to disk.
* This method calls the write_page method of the diskmgr package.
*
* @param pageid the page number in the database.
*/
public void flushPage(PageId pageid) throws ChainException {
	Integer frameNo = directory.get(pageid.pid);
	if(frameNo != null){
		BufDescr bufDesc = bufDescr.get(frameNo);
		Page toFlush = new Page(bufpool[frameNo]);
		if(bufDesc.isDirty()){
				try {
					Minibase.DiskManager.write_page(pageid, toFlush);
				} catch (IOException e) {
					throw new DiskMgrException(e,"DB.java: write_page() failed ");
				}	
				bufDesc.setDirtybit(false);
		}
	}else{
		throw new HashEntryNotFoundException(null,"BUFMGR: Frame not in buffer pool");
	}
	
}
/*
* Used to flush all dirty pages in the buffer pool to disk
*
*/
public void flushAllPages() throws ChainException {
	
	for(BufDescr bufDesc:bufDescr){
		if(bufDesc != null){
		PageId flushPageId = bufDesc.getPageId();
			flushPage(flushPageId);
		}
	}
}
/**
* Returns the total number of buffer frames.
*/
public int getNumBuffers() {return bufpool.length;}
/**
* Returns the total number of unpinned buffer frames.
*/
public int getNumUnpinned() {
	int noUnpinned=0;
	for(BufDescr bufDesc:bufDescr){
			if(bufDesc == null ||  bufDesc.getPin_count() == 0){
				noUnpinned++;
		}
	}
	return noUnpinned;
	}
public Integer getCurrentTime() {
	return timeCounter;
}
public List<BufDescr> getBufDescList() {
	return bufDescr;
}
public HashTable getHashTable() {
	return directory;
}

}
