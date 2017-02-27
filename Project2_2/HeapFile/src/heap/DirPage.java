package heap;
import global.PageId;
import heap.HFPage;
import global.RID;
import global.Page;

import java.awt.image.DirectColorModel;
import java.util.*;

// Referenced classes of package heap:
//            HFPage

class DirPage extends HFPage
{
    HFPage header;
    PageId headPageid;
    public DirPage() {

    }
    public PageId getFirstCorrectPage(int length) {
        return null;
    }
}