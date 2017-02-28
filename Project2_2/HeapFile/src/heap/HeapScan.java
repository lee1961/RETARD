package heap;

import bufmgr.BufMgr;
import global.*;

import java.io.IOException;
import chainexception.ChainException;
//A heap scan is simply an iterator that traverse the file's directory
//and data pages to return all records.
public class HeapScan
    implements GlobalConst {
    HeapFile hf;
    private DirPage next_dir;
    private DirPage current_dir;
    RID next_rid;
    RID curr_rid;
    HFPage current_dataPage;
    int flag_justStartedScanning = 1;

    protected HeapScan(HeapFile hf)

    {
        this.hf = hf;
        Minibase.BufferManager.pinPage(hf.headerId, hf.dir, false);
        //next_rid = hf.dir.firstRecord();
        //curr_rid = hf.dir.firstRecord();


    }

    protected void finalize()
            throws Throwable {
        close();
    }

    public void close() throws ChainException {
        //make sure we unpined any pages
        //need to try catch
        Minibase.BufferManager.unpinPage(hf.headerId, false);
    }

    public boolean hasNext() {
        if (getNext(curr_rid) != null) {
            return true;
        } else {
            return false;
        }

    }

    public Tuple getNext(RID rid) {
        // just initialised
        if ((flag_justStartedScanning == 1)) {
            next_rid = new RID();
            next_rid.copyRID(rid); // pointing towards new one
            current_dataPage = new HFPage();
            current_dir = hf.dir; // point to the head
            flag_justStartedScanning = 0;
        }
        // need to get the first RID
        RID dir_record = current_dir.firstRecord();
        while(current_dir != null) {
            // record of this first directory page
            //RID dir_record = current_dir.firstRecord();
            while(dir_record != null) {
                byte current_byte [] = current_dir.selectRecord(dir_record);
                int current_pid = Convert.getIntValue(0,current_byte);
                // found the correct RID for the initialising part
                if(current_pid == next_rid.pageno.pid) {
                    HFPage hf = new HFPage();
                    Minibase.BufferManager.pinPage(next_rid.pageno,hf,false);
                    byte [] byteToReturn = hf.selectRecord(next_rid);
                    Tuple tuple = new Tuple(byteToReturn,0,byteToReturn.length);
                    Minibase.BufferManager.unpinPage(next_rid.pageno,false);
                    if(hf.hasNext(next_rid)) {
                        boolean f = true;
                    } else {
                        boolean f = false;
                    }

                    next_rid= hf.nextRecord(next_rid);
                    if(next_rid != null) {
                        return tuple;
                    } else {
                        // cant really return the tupple yet
                        // need to go either to the next directory or its really the end

                        // THIS IS THE END CONDITION M8!
                        RID r = current_dir.nextRecord(dir_record);
                        PageId r2 = current_dir.getNextPage();
                        if((current_dir.nextRecord(dir_record) == null) && (current_dir.getNextPage().pid == -1)) {
                            return tuple;
                        }
                    }

                }
                dir_record = current_dir.nextRecord(dir_record);
            }
            //dir_record = current_dir.nextRecord(dir_record);
            //PageId temp_id = new PageId();
            PageId temp_id = current_dir.getNextPage();
            if(temp_id == null) {
                break;
            }
            Minibase.BufferManager.pinPage(temp_id,current_dir,false);
            Minibase.BufferManager.unpinPage(temp_id,false );

        }
        return null;



    }

}