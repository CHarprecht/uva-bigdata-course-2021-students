package nl.uva.bigdata.hadoop.assignment1;

import org.apache.hadoop.io.Writable;

import javax.print.DocFlavor;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class BooksWritable  implements Writable {

    private Book[] books;

    public void setBooks(Book[] books) {
        this.books = books;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.books.length);

        for (int i = 0; i < this.books.length; i = i + 1) {
            out.writeUTF(this.books[i].getTitle());
            out.writeInt(this.books[i].getYear());
        }
        System.out.println("Wrote all "+this.books.length+ " books");
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();

        Book[] books = new Book[length];
        for (int j = 0; j < length; j = j + 1) {
            String title = in.readUTF();
            int year = in.readInt();
            books[j] = new Book(title, year);
        }
        setBooks(books);
        System.out.println("Read all "+length+" books");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BooksWritable that = (BooksWritable) o;
        return Arrays.equals(books, that.books);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(books);
    }
}
