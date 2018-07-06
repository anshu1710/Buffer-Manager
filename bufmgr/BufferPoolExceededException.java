package bufmgr;

import chainexception.ChainException;

public class BufferPoolExceededException extends ChainException {
	
	public BufferPoolExceededException(Exception be, String string) {
		super(be,string);
	}

}
