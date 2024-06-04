package simpledb;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int maxNumPages;
    private Page[] pageBuffer;
    private boolean[] pageBufferUsed;
    private HashMap<PageId, Integer> pageId2Loc;
    private LinkedList<PageId> LRUList;
    // Add for concurrency
    private ConcurrentHashMap<PageId, PageLock> pid2Lock;
    private ConcurrentHashMap<TransactionId, Set<PageId>> tid2Pid;
    private DependencyGraph DG;

    private class DependencyGraph {
        public ConcurrentHashMap<TransactionId, Set<TransactionId>> tidToEdge = new ConcurrentHashMap<>();
        public Set<TransactionId> vis = Collections.synchronizedSet(new HashSet<>());

        public synchronized void modifyEdges(TransactionId tid, PageId pid) {
            tidToEdge.putIfAbsent(tid, new HashSet<>());
            Set<TransactionId> edges = tidToEdge.get(tid);
            edges.clear();
            if (pid == null) return;
            Set <TransactionId> pidToTid;
            synchronized (pid2Lock.get(pid)) {
                pidToTid = pid2Lock.get(pid).relatedTid();
            }
            edges.addAll(pidToTid);
        }

        public boolean DFS(TransactionId cur, TransactionId fa) {
            vis.add(cur);
            Set<TransactionId> edges = tidToEdge.get(cur);
            if (edges == null) return false;
            boolean flag = false;
            for (TransactionId nxt : edges) {
                if (nxt.equals(fa)) return true;
                else if (!vis.contains(nxt)) {
                    flag = flag || DFS(nxt, fa);
                }
            }
            return flag;
        }

        public synchronized boolean isDeadLocked(TransactionId tid) {
            vis.clear();
            return DFS(tid, tid);
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.maxNumPages = numPages;
        this.pageBuffer = new Page[numPages];
        this.pageBufferUsed = new boolean[numPages];
        this.pageId2Loc = new HashMap<>();
        this.LRUList = new LinkedList<>();
        for (int i = 0; i < this.pageBufferUsed.length; i++)
            pageBufferUsed[i] = false;
        this.pid2Lock = new ConcurrentHashMap<>();
        this.tid2Pid = new ConcurrentHashMap<>();
        this.DG = new DependencyGraph();
    }
    
    public static int getPageSize() {
        return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        pid2Lock.putIfAbsent(pid, new PageLock(pid));
        boolean succ;
        synchronized (pid2Lock.get(pid)) {
            succ = pid2Lock.get(pid).acquireLock(perm, tid);
        }
        while (!succ) {
            DG.modifyEdges(tid, pid);
            if (DG.isDeadLocked(tid)) {
                throw new TransactionAbortedException();
            }
            synchronized (pid2Lock.get(pid)) {
                succ = pid2Lock.get(pid).acquireLock(perm, tid);
            }
        }
        DG.modifyEdges(tid, null);
        tid2Pid.putIfAbsent(tid, new HashSet<>());
        tid2Pid.get(tid).add(pid);

        synchronized (this) {
            if (pageId2Loc.containsKey(pid)) {
                LRUList.remove(pid);
                LRUList.addLast(pid);
                return this.pageBuffer[pageId2Loc.get(pid)];
            } 
            else {
                Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                int index = this.pageBuffer.length;
                for (int i = 0; i < this.pageBuffer.length; i++) {
                    if (!this.pageBufferUsed[i]) {
                        index = i;
                        break;
                    }
                }
                if (index >= this.pageBuffer.length)
                {
                    this.evictPage();
                    for (int i = 0; i < this.pageBuffer.length; i++)
                    {
                        if (!this.pageBufferUsed[i])
                        {
                            index = i;
                            break;
                        }
                    }
                }
                page.setBeforeImage();
                this.pageBuffer[index] = page;
                this.pageBufferUsed[index] = true;
                this.pageId2Loc.put(pid, index);
                this.LRUList.addLast(pid);
                return page;
            }
        }
        
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        synchronized (pid2Lock.get(pid)) {
            pid2Lock.get(pid).releaseLock(tid);
        }
        tid2Pid.get(tid).remove(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        synchronized (pid2Lock.get(p)) {
            return pid2Lock.get(p).isHolding(tid);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        Set<PageId> lockedPages = tid2Pid.get(tid);
        tid2Pid.remove(tid);
        if (lockedPages == null) return;
        for (PageId pid : lockedPages) {
            if (pageId2Loc.containsKey(pid)) {
                Page page = pageBuffer[pageId2Loc.get(pid)];
                if (pid2Lock.get(pid).isExclusive()) {
                    if (commit) {
                        if (page.isDirty() != null) {
                            this.flushPage(pid);
                            page.markDirty(false, null);
                            page.setBeforeImage();
                        }
                    } 
                    else {
                        assert page.getBeforeImage() != null;
                        pageBuffer[pageId2Loc.get(pid)] = page.getBeforeImage();
                    }
                }
            }
            synchronized (pid2Lock.get(pid)) {
                pid2Lock.get(pid).releaseLock(tid);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile targetTable = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = targetTable.insertTuple(tid, t);
        synchronized (this) {
            
            for (Page dirtyPage : dirtyPages) {
                PageId pid = dirtyPage.getId();
                if (this.pageId2Loc.containsKey(pid)) {
                    LRUList.remove(pid);
                    LRUList.addLast(pid);
                    pageBuffer[pageId2Loc.get(pid)] = dirtyPage;
                }
                else {
                    int index = pageBuffer.length;
                    for (int i = 0; i < pageBuffer.length; ++i) {
                        if (!pageBufferUsed[i]) {
                            index = i;
                            break;
                        }
                    }
                    while (index >= pageBuffer.length) {
                        this.evictPage();
                        for (int i = 0; i < pageBuffer.length; ++i) {
                            if (!pageBufferUsed[i]) {
                                index = i;
                                break;
                            }
                        }
                    }
                    pageBuffer[index] = dirtyPage;
                    pageBufferUsed[index] = true;
                    pageId2Loc.put(pid, index);
                    LRUList.addLast(pid);
                    dirtyPage.setBeforeImage();
                }
                dirtyPage.markDirty(true, tid);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile targetTable = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirtyPages = targetTable.deleteTuple(tid, t);
        synchronized (this) {
            
            for (Page dirtyPage : dirtyPages) {
                PageId pid = dirtyPage.getId();
                if (this.pageId2Loc.containsKey(pid)) {
                    LRUList.remove(pid);
                    LRUList.addLast(pid);
                    pageBuffer[pageId2Loc.get(pid)] = dirtyPage;
                } 
                else {
                    int index = pageBuffer.length;
                    for (int i = 0; i < pageBuffer.length; ++i) {
                        if (!pageBufferUsed[i]) {
                            index = i;
                            break;
                        }
                    }
                    while (index >= pageBuffer.length) {
                        this.evictPage();
                        for (int i = 0; i < pageBuffer.length; i++) {
                            if (!pageBufferUsed[i]) {
                                index = i;
                                break;
                            }
                        }
                    }
                    pageBuffer[index] = dirtyPage;
                    pageBufferUsed[index] = true;
                    pageId2Loc.put(pid, index);
                    LRUList.addLast(pid);
                    dirtyPage.setBeforeImage();
                }
                dirtyPage.markDirty(true, tid);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        try {
            for (PageId pid : pageId2Loc.keySet()) {
                flushPage(pid);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        if (pageId2Loc.containsKey(pid)) {
            int loc = pageId2Loc.get(pid);
            pageBufferUsed[loc] = false;
            LRUList.remove(pid);
            pageId2Loc.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            if (pageId2Loc.containsKey(pid)) {
                int loc = pageId2Loc.get(pid);
                Page page = pageBuffer[loc];
                if (page.isDirty() == null) return;
                page.markDirty(false, null);
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
                // this.pageBufferUsed[loc] = false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        Iterator<PageId> iterator = LRUList.iterator();
        PageId targetPageId = null;
        while (iterator.hasNext()) {
            PageId cur = iterator.next();
            Page page = pageBuffer[pageId2Loc.get(cur)];
            if (page.isDirty() == null) {
                targetPageId = cur;
                break;
            }
        }
        if (targetPageId == null) throw new DbException("No page to evict");
        int loc = pageId2Loc.get(targetPageId);
        this.pageBufferUsed[loc] = false;
        try {
            this.flushPage(targetPageId);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
        pageId2Loc.remove(targetPageId);
        this.LRUList.remove(targetPageId);    
    }
}