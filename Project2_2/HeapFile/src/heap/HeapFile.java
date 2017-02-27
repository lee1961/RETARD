package heap;
import bufmgr.BufMgr;
import diskmgr.DiskMgr;
import global.*;


import java.io.IOException;
import chainexception.ChainException;
// Referenced classes of package heap:
//            DirPage, DataPage, HeapScan

public class HeapFile
        implements GlobalConst
{
    String heapFileName;
    int recordCount = 0;

    public HeapFile(String name) {
        this.heapFileName = name;
    }
    public RID insertRecord(byte[] record) throws ChainException{

    }
    public Tuple getRecord(RID rid){
        return null;
    }
    public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException{
        return false;
    }
    public boolean deleteRecord(RID Rid){
        return false;
    }
    public int getRecCnt(){
        //get the number of records in the file
        return -1;
    }
    public HeapScan openScan(){
        return null;
    }
}