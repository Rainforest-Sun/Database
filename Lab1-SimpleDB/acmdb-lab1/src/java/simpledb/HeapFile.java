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
    private ConcurrentHashMap<PageId, Page> PageIdToPage;

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
        this.PageIdToPage = new ConcurrentHashMap<>();
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
        // throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        FileInputStream fis = new FileInputStream(file);
        byte[] data = new byte[BufferPool.getPageSize()];
        fis.skip(pid.pageNumber() * BufferPool.getPageSize());
        fis.read(data);
        fis.close();

        Page page = new HeapPage((HeapPageId) pid, data);
        this.PageIdToPage.put(pid, page);
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.PageIdToPage.size();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    /**
     * An auxiliary class that implements the Java Iterator for tuples on a page
     */
    public class HeapFileIterator implements iterator<Tuple> {
        private Iterator<PageId> pageIdIterator;
        private Iterator<Tuple> tupleIterator;
        private TransactionId tid;
        private PageId pid;
        private HeapPage page;

        public HeapFileIterator(TransactionId tid) {
            this.pageIdIterator = PageIdToPage.keySet().iterator();
            this.tupleIterator = null;
            this.tid = tid;
            this.pid = null;
            this.page = null;
        }

        public boolean hasNext() {
            if (this.tupleIterator != null && this.tupleIterator.hasNext()) {
                return true;
            }
            while (this.pageIdIterator.hasNext()) {
                this.pid = this.pageIdIterator.next();
                this.page = (HeapPage) PageIdToPage.get(pid);
                this.tupleIterator = page.iterator();
                if (this.tupleIterator.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        public Tuple next() {
            if (this.tupleIterator == null || !this.tupleIterator.hasNext()) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
            }
            return this.tupleIterator.next();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }


    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

