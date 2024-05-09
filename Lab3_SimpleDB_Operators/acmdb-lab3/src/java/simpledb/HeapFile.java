package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    // private ConcurrentHashMap<PageId, Page> PageIdToPage;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        // this.PageIdToPage = new ConcurrentHashMap<>();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            FileInputStream fis = new FileInputStream(this.file);
            byte[] data = new byte[BufferPool.getPageSize()];
            fis.skip(pid.pageNumber() * BufferPool.getPageSize());
            fis.read(data);
            fis.close();

            Page page = new HeapPage((HeapPageId) pid, data);
            return page;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPageId id = (HeapPageId) page.getId();
		
		byte[] data = page.getPageData();
		RandomAccessFile rf = new RandomAccessFile(this.file, "rw");
		rf.seek(id.pageNumber() * BufferPool.getPageSize()); // Not Sure
        rf.write(data);
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (!this.td.equals(t.getTupleDesc())) throw new DbException("TupleDesc doesn't match");
        ArrayList<Page> modifiedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); ++i) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage tmpPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            try {
                tmpPage.insertTuple(t);
                modifiedPages.add(tmpPage);
                return modifiedPages;
            }
            catch (DbException ignored) {}
        }
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        modifiedPages.add(newPage);
        writePage(newPage);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (!this.td.equals(t.getTupleDesc())) throw new DbException("TupleDesc doesn't match");
        ArrayList<Page> modifiedPages = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage tPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        tPage.deleteTuple(t);
        modifiedPages.add(tPage);
        return modifiedPages;
    }

    /**
     * An auxiliary class that implements the Java Iterator for tuples on a page
     */
    public class HeapFileIterator implements DbFileIterator {
        private Iterator<Tuple> tupleIterator;
        private TransactionId tid;
        private PageId curPid;
        private boolean isOpen;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.isOpen = false;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.curPid = new HeapPageId(getId(), 0);
            this.tupleIterator = ((HeapPage) Database.getBufferPool().getPage(tid, curPid, Permissions.READ_ONLY)).iterator();
            this.isOpen = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!this.isOpen) {
                return false;
            }
            if (this.tupleIterator.hasNext()) {
                return true;
            }
            if (this.curPid.pageNumber() + 1 >= numPages()) {
                return false;
            }
            int nextPageNo = this.curPid.pageNumber() + 1;
            PageId nextPid = new HeapPageId(getId(), nextPageNo);
            Iterator<Tuple> nextTupleIterator = ((HeapPage) Database.getBufferPool().getPage(tid, nextPid, Permissions.READ_ONLY)).iterator();
            return nextTupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (this.tupleIterator.hasNext()) return this.tupleIterator.next();
            this.curPid = new HeapPageId(getId(), this.curPid.pageNumber() + 1);
            this.tupleIterator = ((HeapPage) Database.getBufferPool().getPage(tid, curPid, Permissions.READ_ONLY)).iterator();
            return this.tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            this.isOpen = false;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }
}

