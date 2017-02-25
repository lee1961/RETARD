package bufmgr;

import global.PageId;

/**
 * Created by lee1961 on 2/21/17.
 */
public class bufDescr {
        PageId pageno;
        int pin_count;
        boolean dirtybit;

        bufDescr() {
            this.pageno = null;
            this.pin_count = 1;
            this.dirtybit = false;
        }

        bufDescr(PageId pageno) {
            this.pageno = pageno;
            this.pin_count = 1;
            this.dirtybit = false;
        }

}
