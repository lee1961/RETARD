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
    RID dir_record;

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

//    public Tuple getNext(RID rid) {
//        // just initialised
//        if ((flag_justStartedScanning == 1)) {
//            next_rid = new RID();
//            next_rid.copyRID(rid); // pointing towards new one
//            current_dataPage = new HFPage();
//            current_dir = hf.dir; // point to the head
//            flag_justStartedScanning = 0;
//        }
//        // need to get the first RID
//        RID dir_record = current_dir.firstRecord();
//        while(current_dir != null) {
//            // record of this first directory page
//            //RID dir_record = current_dir.firstRecord();
//            while(dir_record != null) {
//                byte current_byte [] = current_dir.selectRecord(dir_record);
//                int current_pid = Convert.getIntValue(0,current_byte);
//                // found the correct RID for the initialising part
//                if(current_pid == next_rid.pageno.pid) {
//                    HFPage hf = new HFPage();
//                    Minibase.BufferManager.pinPage(next_rid.pageno,hf,false);
//                    byte [] byteToReturn = hf.selectRecord(next_rid);
//                    Tuple tuple = new Tuple(byteToReturn,0,byteToReturn.length);
//                    Minibase.BufferManager.unpinPage(next_rid.pageno,false);
//                    if(hf.hasNext(next_rid)) {
//                        boolean f = true;
//                    } else {
//                        boolean f = false;
//                    }
//
//                    next_rid= hf.nextRecord(next_rid);
//                    if(next_rid != null) {
//                        return tuple;
//                    } else {
//                        // cant really return the tupple yet
//                        // need to go either to the next directory or its really the end
//
//                        // THIS IS THE END CONDITION M8!
//                        RID r = current_dir.nextRecord(dir_record);
//                        PageId r2 = current_dir.getNextPage();
//                        if((current_dir.nextRecord(dir_record) == null) && (current_dir.getNextPage().pid == -1)) {
//                            return tuple;
//                        }
//                    }
//
//                }
//                dir_record = current_dir.nextRecord(dir_record);
//            }
//            //dir_record = current_dir.nextRecord(dir_record);
//            //PageId temp_id = new PageId();
//            PageId temp_id = current_dir.getNextPage();
//            if(temp_id == null) {
//                break;
//            }
//            Minibase.BufferManager.pinPage(temp_id,current_dir,false);
//            Minibase.BufferManager.unpinPage(temp_id,false );
//
//        }
//        return null;
//
//
//
//    }

//    public Tuple getNext(RID rid) {
//        // just initialised
//        if ((flag_justStartedScanning == 1)) {
//            next_rid = new RID();
//            //next_rid.copyRID(rid); // pointing towards new one
//
//            next_rid.copyRID(hf.dir.firstRecord());
//
//
//            current_dataPage = new HFPage();
//            current_dir = hf.dir; // point to the head
//            flag_justStartedScanning = 0;
//            next_rid.slotno = 0;
//
//
//            // ne   ed to point to the correct position first
//
//        }
//
//        if(flag_justStartedScanning == 0) {
//            // closing condition
//            if(next_rid == null) {
//                Minibase.BufferManager.unpinPage(hf.headerId,false);
//                return null;
//            }
//        }
//
//        int x = Minibase.BufferManager.getNumUnpinned();
//
//        // need to get the first RID
//        RID dir_record = current_dir.firstRecord();
//        while(current_dir != null) {
//            // record of this first directory page
//            //RID dir_record = current_dir.firstRecord();
//            while(dir_record != null) {
//                byte current_byte [] = current_dir.selectRecord(dir_record);
//                int current_pid = Convert.getIntValue(0,current_byte);
//                // found the correct RID for the initialising part
//                if(current_pid == next_rid.pageno.pid) {
//                    HFPage hf = new HFPage();
//                    Minibase.BufferManager.pinPage(next_rid.pageno,hf,false);
//                    curr_rid = hf.firstRecord();
//                    byte [] byteToReturn = hf.selectRecord(next_rid);
//                    Tuple tuple = new Tuple(byteToReturn,0,byteToReturn.length);
//                    Minibase.BufferManager.unpinPage(next_rid.pageno,false);
//                    if(hf.hasNext(next_rid)) {
//                        boolean f = true;
//                    } else {
//                        boolean f = false;
//                    }
//                    next_rid= hf.nextRecord(next_rid);
//                    if(next_rid != null) {
//                        return tuple;
//                    } else {
//                        // cant really return the tupple yet
//                        // need to go either to the next directory or its really the end
//
//                        // THIS IS THE END CONDITION M8!
//                        RID r = current_dir.nextRecord(dir_record);
//                        PageId r2 = current_dir.getNextPage();
//                        if((current_dir.nextRecord(dir_record) == null) && (current_dir.getNextPage().pid == -1)) {
//                            return tuple;
//                        }
//                    }
//
//                }
//                dir_record = current_dir.nextRecord(dir_record);
//            }
//            //dir_record = current_dir.nextRecord(dir_record);
//            //PageId temp_id = new PageId();
//            PageId temp_id = current_dir.getNextPage();
//            if(temp_id.pid == -1) {
//                break;
//            }
//            Minibase.BufferManager.pinPage(temp_id,current_dir,false);
//            Minibase.BufferManager.unpinPage(temp_id,false );
//
//        }
//        return null;
//
//
//
//    }


    public Tuple getNext(RID rid) {


        // just initialised
        if ((flag_justStartedScanning == 1)) {
            next_rid = new RID();
            //next_rid.copyRID(rid); // pointing towards new one
            //next_rid.copyRID(hf.dir.firstRecord());
            current_dataPage = new HFPage();
            current_dir = hf.dir; // point to the head


            //next_rid = hf.dir.firstRecord();


            next_rid.slotno = 0;
            dir_record = new RID();
            dir_record = current_dir.firstRecord();

            byte b[] = current_dir.selectRecord(dir_record);
            int first_pid = Convert.getIntValue(0,b);
            PageId p = new PageId(first_pid);
            next_rid.pageno.copyPageId(p);
            next_rid.slotno = 0;
            System.out.println(next_rid.pageno.pid);




            //next_rid = current_dir.firstRecord();
            if(next_rid.pageno.pid == -1) {
                return null;
            }



            // need to point to the correct position first

        }

        if (flag_justStartedScanning == 0) {
            // closing condition
            if (next_rid == null) {
                Minibase.BufferManager.unpinPage(hf.headerId, false);
                rid = null;
                return null;
            }
        }
        flag_justStartedScanning = 0;

        while (dir_record != null) {
            byte current_byte[] = current_dir.selectRecord(dir_record);
            int current_pid = Convert.getIntValue(0, current_byte);
            if (current_pid == next_rid.pageno.pid) {
                HFPage hf = new HFPage();
                Minibase.BufferManager.pinPage(next_rid.pageno, hf, false);
                //curr_rid = hf.firstRecord();
                byte[] byteToReturn = hf.selectRecord(next_rid);
                Tuple tuple = new Tuple(byteToReturn, 0, byteToReturn.length);
                Minibase.BufferManager.unpinPage(next_rid.pageno, false);
                next_rid = hf.nextRecord(next_rid);
                if(next_rid != null ) {
                    //rid = next_rid;
                    return tuple;
                } else {
                    dir_record = current_dir.nextRecord(dir_record); // go to the next
                    //int x = current_dir.getNextPage().pid;
                    if(dir_record == null && current_dir.getNextPage().pid == -1) {
                        next_rid = null;
                        //rid = null;
                        return tuple;
                    } else {
                        byte [] arr = current_dir.selectRecord(dir_record);
                        int pid = Convert.getIntValue(0,arr);
                        PageId the_next_pageID = new PageId(pid);
                        HFPage h = new HFPage();
                        Minibase.BufferManager.pinPage(the_next_pageID,h,false);
                        next_rid = h.firstRecord();
                        Minibase.BufferManager.unpinPage(the_next_pageID,false);
                        return tuple;
                    }



                }

            }

        }

        int x = Minibase.BufferManager.getNumUnpinned();


        return null;


    }

}