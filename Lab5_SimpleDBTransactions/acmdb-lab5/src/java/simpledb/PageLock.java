package simpledb;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class PageLock {
    private PageId pageId;
    private Set<TransactionId> SLocks;
    private TransactionId XLock;

    public PageLock(PageId pageId) {
        this.pageId = pageId;
        SLocks = Collections.synchronizedSet(new HashSet<>());
        XLock = null;
    }

    public PageId getPageId() {
        return pageId;
    }

    public Set<TransactionId> getSLocks() {
        return SLocks;
    }

    public TransactionId getELock() {
        return XLock;
    }

    boolean acquireLock(Permissions perm, TransactionId tid) {
        if (perm.equals(Permissions.READ_ONLY)) {
            if (XLock != null) return XLock.equals(tid);
            else {
                SLocks.add(tid);
                return true;
            }
        }
        else {
            if (XLock != null) return XLock.equals(tid);
            if (SLocks.size() == 0) {
                XLock = tid;
                return true;
            }
            else if (SLocks.size() == 1 && SLocks.contains(tid)) {
                XLock = tid;
                SLocks.clear();
                return true;
            }
            else return false;
        }
    }

    void releaseLock(TransactionId tid) {
        assert XLock == null || tid.equals(XLock), "Illegal lock release operation!";
        if (tid.equals(XLock)) XLock = null;
        else SLocks.remove(tid);
    }

    boolean isHolding(TransactionId tid) {
        return SLocks.contains(tid) || tid.equals(XLock);
    }

    boolean isExclusive() {
        return XLock != null;
    }

    boolean isExclusive(TransactionId tid) {
        return XLock != null && XLock.equal(tid);
    }

    Set<TransactionId> relatedTid() {
        Set<TransactionId> s = new HashSet<>(SLocks);
        if (XLock != null) s.add(XLock);
        return s;
    }
}