
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

    //Hashtable directory;
    Stack<Page> mru_stack;
    DiskMgr diskmgr;
    LinkedList<Integer> mru_list;
    int counter = 0;
    HashMap<Integer, Integer> directory = new HashMap<Integer, Integer>();

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
            //bufferDescriptor[i] = new bufDescr();
        }
//        for (int i = 0; i < numbufs; i++) {
//            bufferDescriptor[i].pageno = null;
//            bufferDescriptor[i].pin_count = 0;
//            bufferDescriptor[i].dirtybit = false;
//        }
        //directory = new Hashtable(127);


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


        int retrieved_number = 0;
        //System.out.println("the retrieved number is " + retrieved_number);
        // if the value returned == -1 then it means it is not inside the hashtable
        if (!directory.containsKey(pageno.pid)) {
            //System.out.println("not here ");

            // need to find an empty spot inside the buffer pool if there is one
            if (bufferIsFull()) {
               // System.out.println("buffer is full at size" + bufferPool.length);
                // if there are no replacement candidates
                if (mru_list.isEmpty()) {
                    throw new HashEntryNotFoundException(null, "hashentry not found");
                } else {
                    // there is a replacement candidate

//                    if (pageno.pid == 100) {
//                        for (int i : mru_list) {
//                            System.out.print(i+ ", ");
//                        }
//                        System.out.println();
//                    } else if(pageno.pid == 101) {
//                        for (int i : mru_list) {
//                            System.out.print(i + ", ");
//                        }
//                        System.out.println();
//                    }
                    int replacement_candidate_index = mru_list.removeFirst();
                    int watched_pid = bufferDescriptor[replacement_candidate_index].pageno.pid;
                    directory.remove(bufferDescriptor[replacement_candidate_index].pageno.pid);
//                    System.out.println("after removign....");
//                    if (pageno.pid == 100 || pageno.pid == 101) {
//                        for (int i : mru_list) {
//                            System.out.print(i + ", ");
//                        }
//                        System.out.println();
//                    }

//                    System.out.println("\n\n\n");
//                    System.out.println("i will be inserting pageno " + pageno.pid);
//                    System.out.println("the replacement candidate index is " + replacement_candidate_index + " and the id is " + bufferDescriptor[replacement_candidate_index].pageno.pid);
                    //System.out.println("mru index is " + replacement_candidate_index);
                    // if the replacemenet candidate had a dirty bit need to write it bac
                    // if th


                    if (bufferDescriptor[replacement_candidate_index].dirtybit == true) {

                        try {

                    //        System.out.println("before writing the pageno " + bufferDescriptor[replacement_candidate_index].pageno + ", value of the replacement candidate index is " + " " + Convert.getIntValue(0, bufferPool[replacement_candidate_index].getpage()));
                            Minibase.DiskManager.write_page(bufferDescriptor[replacement_candidate_index].pageno, bufferPool[replacement_candidate_index]);
                        } catch (IOException ee) {
                            ee.printStackTrace();
                        }

                    }

                    //directory.delete(bufferDescriptor[replacement_candidate_index].pageno); // delete from the hashtable

                    //System.out.println("you are removing " + bufferDescriptor[replacement_candidate_index].pageno);

                    bufDescr b = new bufDescr();
                    //PageId new_id = new PageId(pageno.pid);
                    b.pageno = pageno;
                    b.dirtybit = false;
                    b.pin_count = 1;
                    //bufferDescriptor[replacement_candidate_index] = null;
                    bufferDescriptor[replacement_candidate_index] = b;
                    int directory_before_remove = directory.size();

                    int directory_after_remove = directory.size();

                    try {
                        bufferPool[replacement_candidate_index] = new Page();

                        Minibase.DiskManager.read_page(pageno, bufferPool[replacement_candidate_index]);

                        page.setPage(bufferPool[replacement_candidate_index]);
                    //    System.out.println("the new pagno that i am gonna to write is " + pageno.pid + " and the value is " + Convert.getIntValue(0,bufferPool[replacement_candidate_index].getpage()));
                        directory.put(pageno.pid, replacement_candidate_index);

                        // bufferPool[replacement_candidate_index] = page;
                        //bufferPool[replacement_candidate_index] = new Page();
//                        bufferPool[replacement_candidate_index].copyPage(page);

                    } catch (IOException ee) {
                        ee.printStackTrace();

                    }

             //       System.out.println("the pageno id inserted into the hastable is " + pageno.pid);
                    //directory.insert(pageno,replacement_candidate_index); //insert back into the hashtable

                    //page.setPage(bufferPool[replacement_candidate_index]);

                }

            } else {
                // if the buffer is not full
//                System.out.println("the numbufs is " + numbufs);
//                System.out.println("not full");
                for (int i = 0; i < numbufs; i++) {
                    if (bufferPool[i] == null) {
//                        System.out.println("putting pageno as " + pageno.pid);
//                        System.out.println("putting as position " + i);
                        //Page new_page = new Page();
                        //new_page.copyPage(page);
                    //    System.out.println("When bufferpool is not full, inserting at index: " + i + " ,pageno.id is " + pageno.pid);

                        //directory.insert(pageno,i); // insert into hashtable
                        directory.put(pageno.pid, i);

                        // putting the new bufdescr into there
                        bufDescr new_bufdescr = new bufDescr();
                        new_bufdescr.pin_count = 1;
                        new_bufdescr.pageno = pageno;
                        new_bufdescr.dirtybit = false;
                        bufferDescriptor[i] = new_bufdescr;

                        //page.setPage(bufferPool[i]);
                        //page = bufferPool[i];
                        try {
                            bufferPool[i] = new Page();
                            Minibase.DiskManager.read_page(pageno, bufferPool[i]);
                            //bufferPool[i] = page;
                            page.setPage(bufferPool[i]);


                            break;

                        } catch (IOException e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                }
            }


        } else {
            //its already inside the hashtable
            retrieved_number = directory.get(pageno.pid);
            bufferDescriptor[retrieved_number].pin_count++;
            //page = bufferPool[retrieved_number];
            page.setPage(bufferPool[retrieved_number]);
            //page = bufferPool[retrieved_number];
            // System.out.println("\nbuffer which has alreaedy been pinned is " + retrieved_number);
            // check whether its inside the mrulist, remove it from the list since it is no longer a candidate
            for (int i = 0; i < mru_list.size(); i++) {
                if (mru_list.get(i) == retrieved_number) {
                    //  System.out.println("if its in the mru list  removing  this " + retrieved_number + "\n");
                    //System.out.println("removing candidate index " + retrieved_number);
                    mru_list.remove(i); // remove the element
                    break;
                }
            }
//            try {
//                Minibase.DiskManager.read_page(pageno,page);
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

        }


    }

    public boolean bufferIsFull() {
        for (int i = 0; i < numbufs; i++) {
            if (this.bufferPool[i] == null) {
                return false;
            }
        }
        return true;
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
        //System.out.println("unpining pageno " + pageno.pid);
        //int x = directory.find(pageno);
        //System.out.println( "is it found? " + x);
        int x = 0;
        //System.out.println("unpinng page " + pageno.pid);
        if (!directory.containsKey(pageno.pid)) {
            throw new HashEntryNotFoundException(null, "hashentry not found");
            // need to throw a not found excpetion
        } else {
            x = directory.get(pageno.pid);
            //System.out.println("unpinng page index is at " + x);
            if (dirty == true) {
                bufferDescriptor[x].dirtybit = true;
            }
            if (bufferDescriptor[x].pin_count == 0) {
                //System.out.println( "this is " + bufferDescriptor[x].pageno.pid);
                // need to throw another exeption
                throw new PageUnpinnedException();
            } else {
             //   System.out.println("before decreasing index, x with pincount now is " + bufferDescriptor[x].pin_count);
                bufferDescriptor[x].pin_count--;
                if (bufferDescriptor[x].pin_count == 0) {
               //     System.out.println("index x with pincount 0 is at index: " + x + " ,pageno: " + bufferDescriptor[x].pageno.pid);
                    try {
                        int g = Convert.getIntValue(0,bufferPool[x].getpage());
                 //       System.out.println("the value is " + g);
                    } catch (IOException e) {

                    }
                    //System.out.println("the value is " + Convert.getIntValue(0,bufferPool[x].getpage()));
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
        new_pageId = Minibase.DiskManager.allocate_page(howmany);

        pinPage(new_pageId, firstpage, false);
        if (firstpage == null) {
            System.out.println("crashed");
            Minibase.DiskManager.deallocate_page(new_pageId, howmany);
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
        int x = directory.get(globalPageId.pid);
        if (!directory.containsKey(globalPageId.pid)) {

        } else {
            directory.remove(globalPageId.pid);
            bufferDescriptor[x] = null;
            bufferPool[x] = null;
            for (int i = 0; i < mru_list.size(); i++) {
                if (mru_list.get(i) == x) {
                    mru_list.remove(i);
                    break;
                }
            }

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

        int x = directory.get(pageid.pid);
        if (!directory.containsKey(pageid.pid)) {
            throw new HashEntryNotFoundException(null, "hash entry not fond");
        } else {
            Page p = bufferPool[x];
            try {
                Minibase.DiskManager.write_page(pageid, p);


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
        for (int i = 0; i < bufferDescriptor.length; i++) {
            if (bufferDescriptor[i] == null) {
                counter++;
            }
        }
        return counter;
    }

    public int hashFunction(int value) {
        int position = value % HTSIZE;
        return position;
    }


    class Pair {
        int page_number;
        int frame_number;
    }

}
