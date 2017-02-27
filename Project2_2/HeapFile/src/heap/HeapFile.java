package heap;
import bufmgr.BufMgr;
//import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
//import com.sun.org.apache.bcel.internal.generic.RETURN;
import diskmgr.DiskMgr;
import global.*;


import java.io.IOException;
import chainexception.ChainException;

public class HeapFile
        implements GlobalConst
{
    String heapFileName;
    DirPage dir = null;
    PageId headerId = null;
    int recordCount = 0;
    DiskMgr db = new DiskMgr();
    BufMgr bf = new BufMgr(100,50);


    public HeapFile(String name) throws IOException{
        /*this.heapFileName = name;
        dir = new DirPage();*/
        try {
            headerId = Minibase.DiskManager.get_file_entry(name);

            // if it doesnt find one
            if (headerId == null) {
                dir = new DirPage();


                headerId = Minibase.BufferManager.newPage(dir,1);
                //headerId = bf.newPage(dir,1);
                dir.setCurPage(headerId);
                //System.out.println("Headerid is " + headerId.pid);
                Minibase.DiskManager.add_file_entry(name,headerId);
                Minibase.BufferManager.unpinPage(headerId,true);
                Minibase.BufferManager.flushPage(headerId);
            } else {
                System.out.println("u exist");
                // it exists somewhere
                dir = new DirPage();
               // System.out.println("the header id is " + headerId.pid);
                //headerId = Minibase.DiskManager.get_file_entry(name);
                Minibase.BufferManager.pinPage(headerId,dir,false);
                Minibase.BufferManager.unpinPage(headerId,true);
               // System.out.println("dir id is " + dir.getCurPage().pid);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public RID insertRecord(byte[] record) throws ChainException{

        PageId pointer_to_headPage = headerId;
        DirPage dir_temp = this.dir;

        RID Insert = null;
        int recordSize = record.length;
        if (recordSize > MAX_TUPSIZE) {
            throw new SpaceNotAvailableException("Record size is greater than the size of a page.");
        }
        while(dir_temp != null) {

            RID first_record = dir_temp.firstRecord();
            // if there is no FIRST RECORD! must be a lot of space hahhaha
            if(first_record == null) {
                //  need to create ea new data page
                HFPage add = new HFPage();
                PageId addId = Minibase.BufferManager.newPage(add,1);
                Insert = add.insertRecord(record);
                byte[] bytestoadd = new byte[4];
                //Tuple tuple = new Tuple(bytestoadd,0,bytestoadd.length);
                Convert.setIntValue(addId.pid,0,bytestoadd);
                dir_temp.insertRecord(bytestoadd);
                Minibase.BufferManager.unpinPage(addId,true);
                recordCount++;
                return Insert;

            }

            while(first_record != null) {
                byte current_byte [] = dir_temp.selectRecord(first_record);
                int current_pid = Convert.getIntValue(0,current_byte);
                HFPage f = new HFPage();
                PageId pid = new PageId(current_pid);
                Minibase.BufferManager.pinPage(pid,f,false);
                if (f.getFreeSpace() >= recordSize) {
                    Insert = f.insertRecord(record);
                    Minibase.BufferManager.unpinPage(pid,true);
                    recordCount++;
                    return Insert;
                }
                first_record = dir.nextRecord(first_record);
            }
            pointer_to_headPage = dir_temp.getNextPage();
            Minibase.BufferManager.pinPage(pointer_to_headPage,dir_temp,false);
            Minibase.BufferManager.unpinPage(pointer_to_headPage,false);
        }
        //if there is no space in all the heapPages that stores real data


        //add this record to a new page and add this page to the page directory
        HFPage add = new HFPage();
        PageId addId = Minibase.BufferManager.newPage(add,1);
        Insert = add.insertRecord(record);
        byte[] bytestoadd = null;
        Convert.setIntValue(addId.pid,0,bytestoadd);
        Minibase.BufferManager.unpinPage(addId,true);
        //add.insertRecord(bytestoadd);

        // need to loop through again
        dir_temp = dir; // point back to the header page (the first directory page)
        pointer_to_headPage = headerId; // point back to the header page's id
        while(dir_temp != null) {
            if(dir_temp.getFreeSpace() >= bytestoadd.length) {
                dir_temp.insertRecord(bytestoadd);
                recordCount++;
                return Insert;
            }
            pointer_to_headPage = dir_temp.getNextPage();
        }
        // you are currently at the last directory page and no slots exists
        DirPage new_dir = new DirPage();

        PageId new_dir_id =  Minibase.BufferManager.newPage(new_dir,1);
        new_dir.insertRecord(bytestoadd);
        Minibase.BufferManager.unpinPage(new_dir_id,true);
        dir_temp.setNextPage(new_dir_id);
        recordCount++;
        return Insert;

    }
    public Tuple getRecord(RID rid){
        PageId record_pid = rid.pageno;
        HFPage des = new HFPage();
        Minibase.BufferManager.pinPage(record_pid,des,PIN_MEMCPY);
        try {
            byte r [] = des.selectRecord(rid);
            short offset = des.getSlotOffset(rid.slotno);
            Minibase.BufferManager.unpinPage(record_pid,false);
            return new Tuple(r, offset, r.length);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        return null;
    }
    public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException{
        PageId record_pid = rid.pageno;
        HFPage des = new HFPage();
        PageId temp_id = headerId;
        DirPage dir_temp = this.dir;

        while(dir_temp != null) {
            // need to iterate through each tupples of 'this' directory page
            RID first_record = dir_temp.firstRecord();
            while(first_record != null) {
                byte current_byte [] = dir_temp.selectRecord(first_record);
                int current_pid = Convert.getIntValue(0,current_byte);
                if(current_pid == rid.pageno.pid) {
                    // found the correct pid
                    Minibase.BufferManager.pinPage(rid.pageno,des,PIN_MEMCPY);
                    des.updateRecord(rid,newRecord);
                    Minibase.BufferManager.unpinPage(rid.pageno,true);
                    return true;
                }
                first_record = dir_temp.nextRecord(first_record);
            }
            temp_id = dir_temp.getNextPage();
            Minibase.BufferManager.pinPage(temp_id,dir_temp,PIN_MEMCPY);
            Minibase.BufferManager.unpinPage(temp_id,false);
        }
        return false;
    }
    public boolean deleteRecord(RID rid){
        PageId record_pid = rid.pageno;
        HFPage des = new HFPage();
        PageId temp_id = headerId;
        DirPage dir_temp = this.dir;

        while(dir_temp != null) {
            // need to iterate through each tupples of 'this' directory page
            RID first_record = dir_temp.firstRecord();
            while(first_record != null) {
                byte current_byte [] = dir_temp.selectRecord(first_record);
                int current_pid = Convert.getIntValue(0,current_byte);
                if(current_pid == rid.pageno.pid) {
                    // found the correct pid
                    Minibase.BufferManager.pinPage(rid.pageno,des,PIN_MEMCPY);
                    des.deleteRecord(rid);
                    Minibase.BufferManager.unpinPage(rid.pageno,true);
                    recordCount--;
                    return true;
                }
                first_record = dir_temp.nextRecord(first_record);
            }
            temp_id = dir_temp.getNextPage();
            Minibase.BufferManager.pinPage(temp_id,dir_temp,PIN_MEMCPY);
            Minibase.BufferManager.unpinPage(temp_id,false);
        }
        return false;
    }
    public int getRecCnt(){
        //get the number of records in the file
        return recordCount;
    }
    public HeapScan openScan(){
        return new HeapScan(this);
    }
}