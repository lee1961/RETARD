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
    HeapFile hf;
    DirPage current_dir;
    RID next_rid;
    protected HeapScan(HeapFile hf)
    {
        this.hf = hf;
        Minibase.BufferManager.pinPage(hf.headerId,hf.dir,false);
    }

    protected void finalize()
            throws Throwable
    {
        close();
    }

    public void close() throws ChainException
    {
        //make sure we unpined any pages
        //need to try catch
        Minibase.BufferManager.unpinPage(hf.headerId,false);
    }

    public boolean hasNext()
    {
        if(current_dir == null) {
            return false;
        }
        return true;
    }

    public Tuple getNext(RID rid)
    {
        //need to consider different situations
        while(current_dir != null) {
            RID first_record = current_dir.firstRecord();
            while(first_record != null) {
                byte current_byte [] = current_dir.selectRecord(first_record);
                int current_pid = Convert.getIntValue(0,current_byte);
                if(current_pid == rid.pageno.pid) {
                    HFPage des = new HFPage();
                    Minibase.BufferManager.pinPage(rid.pageno,des,false);
                    byte[] b = des.selectRecord(rid);
                    Minibase.BufferManager.unpinPage(rid.pageno,false);
                    Tuple tuple = new Tuple(b,0,b.length);
                    next_rid = des.nextRecord(rid);


                    return tuple;
                }
            }


        }
        return null;


    }
//        while(current_dir != null) {
//            // need to iterate through each tupples of 'this' directory page
//            RID first_record = current_dir.firstRecord();
//            while(first_record != null) {
//                byte current_byte [] = current_dir.selectRecord(first_record);
//                int current_pid = Convert.getIntValue(0,current_byte);
//                if(current_pid == rid.pageno.pid) {
//                    // found the correct pid
//                    Minibase.BufferManager.pinPage(rid.pageno,des,PIN_MEMCPY);
//                    des.updateRecord(rid,newRecord);
//                    Minibase.BufferManager.unpinPage(rid.pageno,true);
//                    return true;
//                }
//                first_record = dir_temp.nextRecord(first_record);
//            }
//            temp_id = dir_temp.getNextPage();
//            Minibase.BufferManager.pinPage(temp_id,dir_temp,PIN_MEMCPY);
//            Minibase.BufferManager.unpinPage(temp_id,false);
//        }

//        PageId record_pid = rid.pageno;
//        HFPage des = new HFPage();
//        PageId temp_id = hf.headerId;
//        DirPage dir_temp = hf.dir;

//        while(dir_temp != null) {
//            // need to iterate through each tupples of 'this' directory page
//            RID first_record = dir_temp.firstRecord();
//            while(first_record != null) {
//                byte current_byte [] = dir_temp.selectRecord(first_record);
//                int current_pid = Convert.getIntValue(0,current_byte);
//                if(current_pid == rid.pageno.pid) {
//                    // found the correct pid
//
//                }
//                first_record = dir_temp.nextRecord(first_record);
//            }
//            temp_id = dir_temp.getNextPage();
//            Minibase.BufferManager.pinPage(temp_id,dir_temp,PIN_MEMCPY);
//            Minibase.BufferManager.unpinPage(temp_id,false);
//        }
//    }
}