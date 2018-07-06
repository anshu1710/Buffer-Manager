package bufmgr;

import chainexception.ChainException;

public class PageUnpinnedException extends ChainException{

	public PageUnpinnedException(Exception pe, String string) {
		super(pe,string);
	}

}
