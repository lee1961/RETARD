
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


import java.io.IOException;
import chainexception.ChainException;


public class BufMgr implements GlobalConst  {
	public int numbufs;
	public int lookAheadSize;

	public String replacementPolicy;
	public bufDescr[] bufferDescriptor;
	final int HTSIZE = 227;
	Pair[] hashTable;
	Page page_table[];


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
		this.numbufs = numbufs;
		this.lookAheadSize = lookAheadSize;
		this.replacementPolicy = replacementPolicy;
		hashTable = new Pair[HTSIZE];
		page_table = new Page[numbufs];

		//BufMgr buf = new BufMgr(numbufs,lookAheadSize,replacementPolicy);
		//BufMgr buf = null;
		//BufMgr buf = new BufMgr(numbufs);
	};
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
	public void pinPage(PageId pageno, Page page, boolean emptyPage) throws ChainException   {


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

	};
	/**
	* Unpin a page specified by a pageId.
	* This method should be called with dirty==true if the client has
	* modified the page.
	* If so, this call should set the dirty bit
	* for this frame.
	* Further, if pin_count>0, this method should
	* decrement it.	
	*If pin_count=0 before this call, throw an exception
	* to report error.
	*(For testing purposes, we ask you to throw
	* an exception named PageUnpinnedException in case of error.)
	*
	* @param pageno page number in the Minibase.
	* @param dirty the dirty bit of the frame*/
	public void unpinPage(PageId pageno, boolean dirty)  throws ChainException{


	};
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
	* @param howmany total number of allocated new pages.
	*
	* @return the first page id of the new pages.__ null, if error.
	*/
	public PageId newPage(Page firstpage, int howmany) throws IOException,
			ChainException {
		return null;
	};
	/**
	* This method should be called to delete a page that is on disk.
	* This routine must call the method in diskmgr package to
	* deallocate the page.
	*
	* @param globalPageId the page number in the data base.
	*/
	public void freePage(PageId globalPageId) throws ChainException{};
	/**
	* Used to flush a particular page of the buffer pool to disk.
	* This method calls the write_page method of the diskmgr package.
	*
	* @param pageid the page number in the database.
	*/
	public void flushPage(PageId pageid) throws ChainException {};
	/**
	* Used to flush all dirty pages in the buffer pool to disk
	*
	*/
	public void flushAllPages() throws ChainException {};
	/**
	* Returns the total number of buffer frames.
	*/
	public int getNumBuffers() {

		return 1;
	}
	/*** Returns the total number of unpinned buffer frames.
	*/
	public int getNumUnpinned() {
		return 2;
	}

	public int hashFunction(int value) {
		int position = value % HTSIZE;
		return position;
	}
	

	class bufDescr {
		PageId pageno;
		int pin_count;
		boolean dirtybit;
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