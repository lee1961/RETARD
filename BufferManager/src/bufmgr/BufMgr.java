
package bufmgr;

import diskmgr.DiskMgr;
import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;


import java.io.IOException;

import chainexception.ChainException;


public class BufMgr implements GlobalConst {
    public int numbufs;
    public int lookAheadSize;

    public String replacementPolicy;

    bufDescr[] bufferDescriptor;
    Page[] bufferPool;

    final int HTSIZE = 227;
    Pair[] hashTable;

    Hashtable directory;
    Stack<Page> mru_stack;
    DiskMgr diskmgr;
    LinkedList<Integer> mru_list;
    int counter = 0;
    
    /*
    * Create the BufMgr object.
    * Allocate pages (frames) for the buffer pool in main memory and
    * make the buffer manage aware that the replacement policy is
    * specified by replacerArg (e.g., LH, Clock, LRU, MRU, LFU, etc.).
    *
    * @param numbufs number of buffers in the buffer pool
    * @param lookAheadSize: Please ignore this parameter
    * @param replacementPolicy Name of the replacement policy, that parameter will be set to "MRU" (you
    can safely ignore this parameter as you will implement only one policy)
    */
    public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {
        //System.out.println("the person typed " + replacementPolicy);

        this.bufferDescriptor = new bufDescr[numbufs];
        this.bufferPool = new Page[numbufs];



        this.numbufs = numbufs;
        this.lookAheadSize = lookAheadSize;
        this.replacementPolicy = replacementPolicy;
        diskmgr = new DiskMgr();
        mru_list = new LinkedList<Integer>();



        for (int i = 0; i < numbufs; i++) {
            bufferPool[i] = null;
        }
//        for (int i = 0; i < numbufs; i++) {
//            bufferDescriptor[i].pageno = null;
//            bufferDescriptor[i].pin_count = 0;
//            bufferDescriptor[i].dirtybit = false;
//        }
        directory = new Hashtable(127);


        //BufMgr buf = new BufMgr(numbufs,lookAheadSize,replacementPolicy);
        //BufMgr buf = null;
        //BufMgr buf = new BufMgr(numbufs);
    }

    ;

    /*
    * Pin a page.
    * First check if this page is already in the buffer pool.
    * If it is, increment the pin_count and return a pointer to this
    * page.
    * If the pin_count was 0 before the call, the page was a
    * replacement candidate, but is no longer a candidate.
    * If the page is not in the pool, choose a frame (from the
    * set of replacement candidates) to hold this page, read the
    * page (using the appropriate method from {\em diskmgr} package) and pin it.
    * Also, must write out the old page in chosen frame if it is dirty
    * before reading new page.__ (You can assume that emptyPage==false for
    * this assignment.)
    *

    * @param pageno page number in the Minibase.
    * @param page the pointer point to the page.
    * @param emptyPage true (empty page); false (non-empty page)
    */
    public void pinPage(PageId pageno, Page page, boolean emptyPage) throws ChainException {

        int retrieved_frame_number = directory.find(pageno);


        try {


            if (retrieved_frame_number == -1) {
                throw new HashEntryNotFoundException();
            } else {
                bufferDescriptor[retrieved_frame_number].pin_count++;
                for(int candidate_position : mru_list) {
                    if(mru_list.get(candidate_position) == retrieved_frame_number) {
                        mru_list.remove(candidate_position);
                        break;
                    }
                }
                page = bufferPool[retrieved_frame_number];
            }

        } catch (HashEntryNotFoundException e) {

            //find the page from disk first
            int full_flag = 0;

            // check whether there is any empty frame in the bufferpool
            for (int i = 0; i < bufferPool.length; i++) {
                // need to change this sh** algorithm to be MRU later
                if (bufferPool[i] == null) {
                    try {
                        Minibase.DiskManager.read_page(pageno, page);
                        bufferPool[i] = page;
                        if(pageno == null) {
                            System.out.println("in here");
                        }
                        //System.out.println("before crashing");
                        //bufferDescriptor[i].pageno = new PageId();
                        //PageId p_id = new PageId(pageno.pid);
                        bufDescr b = new bufDescr(pageno);
                        bufferDescriptor[i] = b;
                        //bufferDescriptor[i].pageno = new bufDescr(p_id);

                        //System.out.println("did i crash here");
//                        bufferDescriptor[i].dirtybit = false;
//                        bufferDescriptor[i].pin_count = 1;
                        directory.insert(pageno, i);


                        throw new IOException("hey");
                    } catch (IOException e2) {
                        //e2.printStackTrace();
                    }

                }
                if (i == bufferPool.length - 1 && bufferPool[i] != null) {
                    full_flag = 1;
                }
            }
            // now use the MRU policy
            if (full_flag == 1) {
                try {
                    if(mru_list.isEmpty()) {
                        throw new BufferPoolExceededException();
                    } else {
                        int x = mru_list.getFirst();
                        mru_list.removeFirst();
                        if(bufferDescriptor[x].dirtybit == true) {
                            try {
                                Minibase.DiskManager.write_page(bufferDescriptor[x].pageno,bufferPool[x]);
                            } catch (IOException io) {

                            }

                        }
                        PageId temp_pageid = bufferDescriptor[x].pageno;
                        directory.delete(temp_pageid);
                        bufferPool[x] = page;
                        bufferDescriptor[x].pageno = pageno;
                        bufferDescriptor[x].dirtybit = false;
                        bufferDescriptor[x].pin_count = 1;

                    }

                } catch (BufferPoolExceededException bufferexception) {
                    page = null;
                    bufferexception.getMessage();
                }
            }

        }

        // try {
        // 	//throw new ChainException();
        // }  catch (ChainException e)  {

        // } catch (IllegalArgumentException e) {

        // }
        // try {

        // } catch (ChainException e) {

        // } catch(Exception e) {

        // }

        // try {

        // } catch (Exception e) {

        // }

        // try {

        // } catch (ChainException e) {

        // } catch (Exception e) {

        // }

    }

    ;

    /**
     * Unpin a page specified by a pageId.
     * This method should be called with dirty==true if the client has
     * modified the page.
     * If so, this call should set the dirty bit
     * for this frame.
     * Further, if pin_count>0, this method should
     * decrement it.
     * If pin_count=0 before this call, throw an exception
     * to report error.
     * (For testing purposes, we ask you to throw
     * an exception named PageUnpinnedException in case of error.)
     *
     * @param pageno page number in the Minibase.
     * @param dirty  the dirty bit of the frame
     */
    public void unpinPage(PageId pageno, boolean dirty) throws ChainException {
        //directory.find(pageno
        int x = directory.find(pageno);
        System.out.println( "the x is " + x);
        if(x == -1) {
            throw new HashEntryNotFoundException();
            // need to throw a not found excpetion
        } else {
            if(dirty == true) {
                bufferDescriptor[x].dirtybit = true;
            }
            if(bufferDescriptor[x].pin_count == 0) {
                System.out.println( "this is " + bufferDescriptor[x].pageno.pid);
                // need to throw another exeption
                throw new PageUnpinnedException();
            } else {
                bufferDescriptor[x].pin_count--;
                if(bufferDescriptor[x].pin_count == 0) {
                    mru_list.addFirst(x);
                }
            }


        }

    }



    /**
     * Allocate new pages.
     * Call DB object to allocate a run of new pages and
     * find a frame in the buffer pool for the first page
     * and pin it. (This call allows a client of the Buffer Manager
     * to allocate pages on disk.) If buffer is full, i.e., you
     * can't find a frame for the first page, ask DB to deallocate
     * all these pages, and return null.
     *
     * @param firstpage the address of the first page.
     * @param howmany   total number of allocated new pages.
     * @return the first page id of the new pages.__ null, if error.
     */
    public PageId newPage(Page firstpage, int howmany) throws IOException,
            ChainException {
        PageId new_pageId = new PageId();
        Minibase.DiskManager.allocate_page(new_pageId,howmany);
        pinPage(new_pageId,firstpage,false);
        if(firstpage == null) {
            Minibase.DiskManager.deallocate_page(new_pageId,howmany);
            return null;
        }
        return new_pageId;

    }

    ;

    /**
     * This method should be called to delete a page that is on disk.
     * This routine must call the method in diskmgr package to
     * deallocate the page.
     *
     * @param globalPageId the page number in the data base.
     */
    public void freePage(PageId globalPageId) throws ChainException {
        Minibase.DiskManager.deallocate_page(globalPageId);
        int x = directory.find(globalPageId);
        if(x == -1) {

        } else {
            directory.delete(globalPageId);
            bufferDescriptor[x] = null;
            bufferPool[x] = null;
            mru_list.remove(x);
        }

    }

    ;

    /**
     * Used to flush a particular page of the buffer pool to disk.
     * This method calls the write_page method of the diskmgr package.
     *
     * @param pageid the page number in the database.
     */
    public void flushPage(PageId pageid) throws ChainException {

        int x= directory.find(pageid);
        if(x == -1) {
            throw new HashEntryNotFoundException();
        } else {
            Page p = bufferPool[x];
            try {
                Minibase.DiskManager.write_page(pageid,p);

                
            } catch (IOException io) {

            }

        }


    }

    ;

    /**
     * Used to flush all dirty pages in the buffer pool to disk
     */
    public void flushAllPages() throws ChainException {
    }

    ;

    /**
     * Returns the total number of buffer frames.
     */
    public int getNumBuffers() {
        return numbufs;
//        int counter = 0;
//        for(int i = 0 ; i < bufferPool.length ;i++) {
//            if(bufferPool[i] != null) {
//                counter++;
//            }
//        }
    }

    /***
     * Returns the total number of unpinned buffer frames.
     */
    public int getNumUnpinned() {
        int counter = 0;
        for(int i = 0 ; i < bufferDescriptor.length ; i++) {
            if(bufferDescriptor[i].pin_count == 0) {
               counter++;
            }
        }
        return counter;
    }

    public int hashFunction(int value) {
        int position = value % HTSIZE;
        return position;
    }


    class bufDescr {
        PageId pageno;
        int pin_count;
        boolean dirtybit;

        bufDescr() {
            this.pageno = null;
            this.pin_count = 0;
            this.dirtybit = false;
        }

        bufDescr(PageId pageno) {
            this.pageno = pageno;
            this.pin_count = 0;
            this.dirtybit = false;
        }
    }

    class Pair {
        int page_number;
        int frame_number;
    }

}
