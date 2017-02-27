package heap;

import global.PageId;

// Referenced classes of package heap:
//            HFPage

class DirPage extends HFPage
{

    public DirPage(){}
    public short getEntryCnt(){
        return 1;
    }
    public void setEntryCnt(short entryCnt){}
    public PageId getPageId(int slotno){
        return null;
    }
    public void setPageId(int slotno, PageId pageno){}
    public short getRecCnt(int slotno){
        return 1;
    }
    public void setRecCnt(int slotno, short recCnt){}
    public short getFreeCnt(int slotno){
        return 1;
    }
    public void setFreeCnt(int slotno, short freeCnt){}
    public void compact(int slotno){}
}