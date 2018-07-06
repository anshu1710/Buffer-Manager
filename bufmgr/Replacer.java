package bufmgr;

import java.io.IOException;
import chainexception.ChainException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;

public  interface Replacer {
    public Integer getEmptyFrame() throws  ChainException;
}
