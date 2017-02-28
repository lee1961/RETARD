package heap;
import bufmgr.BufMgr;
//import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
//import com.sun.org.apache.bcel.internal.generic.RETURN;
import diskmgr.DiskMgr;
import global.*;


import java.io.IOException;
import java.util.MissingFormatArgumentException;

import chainexception.ChainException;

import static heap.HFPage.SLOT_SIZE;

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
            // if there is no FIRST RECORD! need to add a new dataPage
            if(first_record == null) {
                // creating a new datapage as there are no hfpage eh since this will be the first
                HFPage new_dataPage = new HFPage();
                PageId addId = Minibase.BufferManager.newPage(new_dataPage,1);
                new_dataPage.setCurPage(addId);
                //Insert.copyRID(new_dataPage.insertRecord(record));
                Insert = new_dataPage.insertRecord(record);

                // inserting the pointer
                byte[] bytestoadd = new byte[8];
                //inserting the PID to be the first field
                Convert.setIntValue(addId.pid,0,bytestoadd);
                //inserting the amount of free space left inside
                if(new_dataPage.getFreeSpace() < (record.length + SLOT_SIZE)) {
                    throw new SpaceNotAvailableException("ur record is 2 big! to insert into a new heappage");
                }

                // now update how much free space it has left into the page directory pointer
                Convert.setIntValue(new_dataPage.getFreeSpace() + SLOT_SIZE,4,bytestoadd);
                // inserting the pointer into the current page directory
                dir_temp.insertRecord(bytestoadd);
                //unpinning the new dataPage ID created
                Minibase.BufferManager.unpinPage(addId,true);
                recordCount++;
                return Insert;

            }

            while(first_record != null) {
                byte current_byte [] = dir_temp.selectRecord(first_record);
                int current_pid = Convert.getIntValue(0,current_byte);
                int current_freeSpace = Convert.getIntValue(4,current_byte);
                // if this DataPage ID has enough space insert this record there
                if( current_freeSpace >= (record.length + SLOT_SIZE)) {

                    PageId dataPage_pid = new PageId(current_pid);
                    HFPage current_dataPage = new HFPage();
                    current_dataPage.setCurPage(dataPage_pid);
                    Minibase.BufferManager.pinPage(dataPage_pid,current_dataPage,false);
                    Insert = current_dataPage.insertRecord(record);
                    Minibase.BufferManager.unpinPage(dataPage_pid,true);
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


        //create a new HFPAGE object entirely
        HFPage new_dataPage = new HFPage();
        PageId new_dataPageID = new PageId();
        new_dataPageID = Minibase.BufferManager.newPage(new_dataPage,1);

        new_dataPage.setCurPage(new_dataPageID);

        Insert = new_dataPage.insertRecord(record);

        // creating the new pointer to be inserted into the directory page
        byte[] bytestoadd = new byte[8];
        Convert.setIntValue(new_dataPageID.pid,0,bytestoadd);
        Convert.setIntValue(new_dataPage.getFreeSpace(),4,bytestoadd);


        Minibase.BufferManager.unpinPage(new_dataPageID,true);


        // need to loop through again
        dir_temp = dir; // point back to the header page (the first directory page)
        pointer_to_headPage = headerId; // point back to the header page's id
        while(dir_temp != null) {
            if(dir_temp.getFreeSpace() >= (bytestoadd.length + SLOT_SIZE)) {
                Minibase.BufferManager.pinPage(pointer_to_headPage,dir_temp,false);
                dir_temp.insertRecord(bytestoadd);
                Minibase.BufferManager.unpinPage(pointer_to_headPage,true);
                recordCount++;
                return Insert;
            }
            pointer_to_headPage = dir_temp.getNextPage();
            //moving to the next directory if there is no space in the current page directory
            Minibase.BufferManager.pinPage(pointer_to_headPage,dir_temp,false);
            Minibase.BufferManager.unpinPage(pointer_to_headPage,false);
        }

        // you are currently at the last directory page and all the directory pages are already full
        DirPage new_dir = new DirPage();
        PageId new_dir_id =  Minibase.BufferManager.newPage(new_dir,1);
        new_dir.setCurPage(new_dir_id);
        new_dir.insertRecord(bytestoadd);
        Minibase.BufferManager.unpinPage(new_dir_id,true);
        dir_temp.setNextPage(new_dir_id); // point the last directory to point towards the new directory
        recordCount++;
        return Insert;

    }
    public Tuple getRecord(RID rid){
        HFPage current_dataPage = new HFPage();
        PageId current_pid = new PageId(rid.pageno.pid);
        Minibase.BufferManager.pinPage(current_pid,current_dataPage,false);
        if(current_dataPage == null) {
            return null; // no such dataPage exist
        }
        byte [] current_record = current_dataPage.selectRecord(rid);
        Tuple tuple = new Tuple(current_record,0,current_record.length);
        Minibase.BufferManager.unpinPage(current_pid,true);
        return tuple;
    }
    public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException{

        HFPage dataPage_toUpdate = new HFPage();
        PageId toUpdatePid = new PageId(rid.pageno.pid);

        Minibase.BufferManager.pinPage(toUpdatePid,dataPage_toUpdate,false);
        byte [] current_record = dataPage_toUpdate.selectRecord(rid);

        int updated_record_length = newRecord.getLength();
        if(current_record.length != updated_record_length) {
            //update failed
            Minibase.BufferManager.unpinPage(toUpdatePid,false);
            throw new InvalidUpdateException();
        }
        //updated can proceed
        Tuple new_tuple = new Tuple(newRecord.data,0,newRecord.length);

        dataPage_toUpdate.updateRecord(rid,newRecord);
        Minibase.BufferManager.unpinPage(toUpdatePid,true);
        return true;

    }
    public boolean deleteRecord(RID rid){
        HFPage current_dataPage = new HFPage();
        PageId toDeletePid = new PageId(rid.pageno.pid);
        Minibase.BufferManager.pinPage(toDeletePid,current_dataPage,false);
        if(current_dataPage == null) {
            return false; // no such dataPage exist
        }
        current_dataPage.deleteRecord(rid);
        Minibase.BufferManager.unpinPage(toDeletePid,true);
        return true;
    }
    public int getRecCnt(){
        //get the number of records in the file
        return recordCount;
    }
    public HeapScan openScan(){
        return new HeapScan(this);
    }
}