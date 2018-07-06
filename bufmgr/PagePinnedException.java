package bufmgr;

import chainexception.ChainException;

public class PagePinnedException extends ChainException {
	
	public PagePinnedException(Exception pe, String string) {
		super(pe,string);
	}
}
