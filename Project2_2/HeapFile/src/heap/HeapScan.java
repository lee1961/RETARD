package heap;

import bufmgr.BufMgr;
import global.*;

import java.io.IOException;
import chainexception.ChainException;
//A heap scan is simply an iterator that traverse the file's directory
//and data pages to return all records.
public class HeapScan
    implements GlobalConst
{

    protected HeapScan(HeapFile hf)
    {
    }

    protected void finalize()
        throws Throwable
    {
    }

    public void close() throws ChainException
    {
    }

    public boolean hasNext()
    {
        return false;
    }

    public Tuple getNext(RID rid)
    {
        return null;
    }
}