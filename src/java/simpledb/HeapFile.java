package simpledb;

import java.io.*;
import java.util.*;

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

    private final File file;
    private final TupleDesc td;
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
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pageNum = pid.getPageNumber();
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(file, "r");
            if((pageNum + 1) * BufferPool.getPageSize() > f.length()) {
                f.close();
                throw new IllegalArgumentException(String.format("page %d for table %d is invalid", pageNum, tableId));
            } else {
                byte[] data = new byte[BufferPool.getPageSize()];
                f.seek(pageNum * BufferPool.getPageSize());
                int readLen = f.read(data, 0, BufferPool.getPageSize());
                if(readLen != BufferPool.getPageSize())
                    throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pageNum, readLen));
                Page pg = new HeapPage(new HeapPageId(tableId, pageNum), data);
                return pg;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pageNum));
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
        int num = (int)Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
        return num;
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

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

    private static final class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;
        private final HeapFile heapFile;
        private int curPageNum;
        private Iterator<Tuple> it;

        public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
            this.tid = tid;
            this.heapFile = heapFile;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            curPageNum = 0;
            it = getPageTuples(curPageNum);
        }

        private Iterator<Tuple> getPageTuples(int curPageNum) throws TransactionAbortedException, DbException {
            if(curPageNum >= 0 && curPageNum < heapFile.numPages()) {
                PageId pageId = new HeapPageId(heapFile.getId(), curPageNum);
                HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                return pg.iterator();
            } else
                throw new DbException(String.format("heapfile %d page %d pageNum invalid", heapFile.getId(), curPageNum));
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it == null)
                return false;
            if(!it.hasNext()) {
                if(curPageNum < (heapFile.numPages() - 1)) {
                    curPageNum++;
                    it = getPageTuples(curPageNum);
                    return it.hasNext();
                } else
                    return false;
            } else
                return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(it == null || !it.hasNext())
                throw new NoSuchElementException();
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }

}

