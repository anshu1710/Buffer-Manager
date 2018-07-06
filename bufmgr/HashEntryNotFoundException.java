package bufmgr;

import chainexception.ChainException;

public class HashEntryNotFoundException extends ChainException {
	
	public HashEntryNotFoundException(Exception he, String string) {
		super(he,string);
	}

}
