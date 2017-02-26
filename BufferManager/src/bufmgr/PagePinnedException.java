package bufmgr;

import chainexception.ChainException;

/**
 * Created by lee1961 on 2/26/17.
 */
public class PagePinnedException extends ChainException {
    public PagePinnedException(Exception e, String message) {
        super(null,"page cannot be freed when pinned");
    }
}
