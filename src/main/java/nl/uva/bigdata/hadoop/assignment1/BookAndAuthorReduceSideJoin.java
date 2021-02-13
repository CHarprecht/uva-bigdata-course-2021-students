package nl.uva.bigdata.hadoop.assignment1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;


public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job job;

    if (onCluster) {
      job = new Job(jobConf);
    } else {
      job = new Job();
    }
    System.out.println("Start Job Configuration");
    Configuration conf = job.getConfiguration();

    job.setJarByClass(BookAndAuthorReduceSideJoin.class);

    MultipleInputs.addInputPath(job, authors, TextInputFormat.class, AuthorMapper.class);
    MultipleInputs.addInputPath(job, books, TextInputFormat.class, BookMapper.class);

    job.setReducerClass(BookAuthorReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(BookAuthorWritable.class);
    job.setOutputKeyClass(TextOutputFormat.class);
    job.setOutputValueClass(NullWritable.class);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.waitForCompletion(true);
    return 0;
  }
  static class BookAuthorWritable implements WritableComparable {

    private String book = "NA";
    private String author = "NA";
    private int year = 0;

    public BookAuthorWritable() {}

    public String getBook() {
      return book;
    }

    public void setBook(String book) {
      this.book = book;
    }

    public String getAuthor() {
      return author;
    }

    public void setAuthor(String author) {
      this.author = author;
    }

    public int getYear() {
      return year;
    }

    public void setYear(int year) {
      this.year = year;
    }


    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(this.book);
      out.writeUTF(this.author);
      out.writeInt(this.year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.book = in.readUTF();
      this.author = in.readUTF();
      this.year = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BookAuthorWritable that = (BookAuthorWritable) o;
      return book == that.book && author == that.author && year == that.year;
    }

    @Override
    public int hashCode() {
      return Objects.hash(book, author, year);
    }

    @Override
    public int compareTo(Object o) {
      BookAuthorWritable other = (BookAuthorWritable) o;
      int byBook = this.book.compareTo(other.book);
      int byYear = Integer.compare(this.year, other.year);

      if ((byBook == 0) && (byYear == 0)) {
        return this.author.compareTo(other.author);
      } else {
        return byBook;
      }
    }
  }

  public static class AuthorMapper extends Mapper<Object, Text, IntWritable, BookAuthorWritable> {
    BookAuthorWritable output = new BookAuthorWritable();
    IntWritable id = new IntWritable();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
      System.out.println("-----Start Author Mapping with: "+value);

      StringTokenizer tokenizer = new StringTokenizer(value.toString());

      id.set(Integer.parseInt(tokenizer.nextToken()));

      String author = tokenizer.nextToken();
      while(tokenizer.hasMoreTokens()){
        author = author + " "+ tokenizer.nextToken();
      }

      output.setAuthor(author);
      System.out.println("ID: "+id+" Author: "+output.getAuthor());
      context.write(id, output);
    }
  }
  public static class BookMapper extends Mapper<Object, Text, IntWritable, BookAuthorWritable> {
    BookAuthorWritable output = new BookAuthorWritable();
    IntWritable id = new IntWritable();


    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(value.toString());

      id.set(Integer.parseInt(tokenizer.nextToken()));
      int year = Integer.parseInt(tokenizer.nextToken());

      String book = tokenizer.nextToken();
      while(tokenizer.hasMoreTokens()){
        book = book + " "+ tokenizer.nextToken();
      }

      output.setBook(book);
      output.setYear(year);

      System.out.println("ID: "+id+" Book: "+book+" Year: "+year);
      context.write(id,output);
    }
  }

  public static class BookAuthorReducer extends Reducer<IntWritable,BookAuthorWritable,Text,NullWritable> {
    BookAuthorWritable output = new BookAuthorWritable();
    ArrayList<BookAuthorWritable> books;

    public void reduce(IntWritable id,Iterable<BookAuthorWritable> inputs,  Context context)
            throws IOException, InterruptedException {
      System.out.println("-----Received Input: "+inputs + " with id: "+id);

      for (BookAuthorWritable input : inputs) {
        String author = input.getAuthor();
        System.out.println("-----Received Author: "+author + " with id: "+id);
        if (!author.equals("NA")) {
          output.setAuthor(author);
        }
        else {
          books.add(input);
        }
      }
      for (int i = 0; i < books.size(); i ++) {
        BookAuthorWritable input = books.get(i);
        String book = input.getBook();
        int year = input.getYear();
        System.out.println("-----Received Book: "+book + " with id: "+id+" and year: "+year);
        if (!book.equals("NA")) {
          output.setBook(book);
          output.setYear(year);
        }
        if (!output.getBook().equals("NA") && !output.getAuthor().equals("NA")) {
          Text textOut = new Text(output.getAuthor()+"\t"+output.getBook()+"\t"+output.getYear());
          System.out.println("Final Output: "+textOut);
          context.write(textOut,NullWritable.get());
        }
      }

      }

    }

}