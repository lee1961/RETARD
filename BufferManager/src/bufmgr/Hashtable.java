

package bufmgr;
import global.Page;
import global.PageId;

public class Hashtable {

	PageId id;
	HashEntry[] hashEntries;
	int HTSIZE;

	public Hashtable(int size) {
		this.HTSIZE = size;
		this.hashEntries = new HashEntry[this.HTSIZE];

	}
	public int hash(int x) {

		return (3 *x + 5) % this.HTSIZE;
	}

	public void insert(PageId insert_page, int x) {
		// int hash = (myhash( key ) % TABLE_SIZE);
		int hash_value = hash(insert_page.pid);
		if (hashEntries[hash_value] == null)
			hashEntries[hash_value] = new HashEntry(insert_page,x);
		else
		{
			HashEntry entry = hashEntries[hash_value];
			while (entry.next != null && !(entry.page_id.pid == insert_page.pid))
				entry = entry.next;
			if (entry.page_id.pid == insert_page.pid)
				entry.frame_id = x;
			else
				entry.next = new HashEntry(insert_page,x);
		}

	}

	public boolean contain(PageId insert_page) {
		int hash_value = hash(insert_page.pid);
		if (hashEntries[hash_value] == null)
			return false;
		else
		{
			HashEntry entry = hashEntries[hash_value];
			while (entry != null && !(entry.page_id.pid == insert_page.pid))
				entry = entry.next;
			if (entry == null)
				return false;
			else
				return true;
		}
	}

	public boolean delete(PageId insert_page) {
 		int hash_value = hash(insert_page.pid);
        if (hashEntries[hash_value] != null)
        {
            HashEntry prevEntry = null;
            HashEntry entry = hashEntries[hash_value];
            while (entry.next != null && !(entry.page_id.pid == insert_page.pid))
            {
                prevEntry = entry;
                entry = entry.next;
            }
            if (entry.page_id.pid == insert_page.pid)
            {

                if (prevEntry == null)
                    hashEntries[hash_value] = entry.next;
                else
                    prevEntry.next = entry.next;
                return true;
            } else {
            	return false;
            }

        } else {
        	return false;
        }
	}
	public int find(PageId insert_page) {
		int hash_value = hash(insert_page.pid);
        if (hashEntries[hash_value] == null)
            return -1;
        else
        {
            HashEntry entry = hashEntries[hash_value];
            while (entry != null && !(entry.page_id.pid == insert_page.pid))
                entry = entry.next;
            if (entry == null)
                return -1;
            else
                return entry.frame_id;
        }
	}

	class HashEntry
	{
		PageId page_id;
		int frame_id;
		HashEntry next;

		/* Constructor */
		HashEntry(PageId page_id, int frame_id)
		{
			this.page_id = page_id;
			this.frame_id = frame_id;
			this.next = null;
		}
	}

}
