package bufmgr;
import java.time.Instant;
import java.util.*;

import global.PageId;

public class BufDescr {

	private PageId pageId;
	private long pin_count;
	private boolean dirtybit;
	private List<Long> times;

	public BufDescr() {
		this.pageId = new PageId(-1);
		this.pin_count = 0;
		this.dirtybit = false;
		this.times = new ArrayList<Long>();
	}
	
	public void reset(){
		pageId = new PageId(-1);
		pin_count = 0;
		dirtybit = false;
		times = new ArrayList<Long>();
	}

	public PageId getPageId() {
		return this.pageId;
	}

	public void setPageId(Integer pageId) {
		this.pageId = new PageId(pageId);
	}

	public long getPin_count() {
		return this.pin_count;
	}

	public void setPin_count(long pin_count) {
		if (pin_count >= 0)
			this.pin_count = pin_count;
	}

	public boolean isDirty() {
		return this.dirtybit;
	}

	public void setDirtybit(boolean dirtybit) {
		this.dirtybit = dirtybit;
	}

	public void incrPinCount() {
		this.pin_count++;
	}
	public List<Long> getAccessTimes(){
		return times;
	}
	
	public void decrPinCount() throws PageUnpinnedException {
		if (this.pin_count != 0) {
			this.pin_count--;
		}else{
			throw new PageUnpinnedException (null, "BUFMGR: PAGE_NOT_PINNED.");
		}
	}
	
	public void addAccessTime(){
		
		long now = Instant.now().toEpochMilli();
		times.add(now);
	}
}

