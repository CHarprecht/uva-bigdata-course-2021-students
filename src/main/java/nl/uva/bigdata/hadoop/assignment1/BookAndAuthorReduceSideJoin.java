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
      return this.book;
    }

    public void setBook(String book) {
      this.book = book;
    }

    public String getAuthor() {
      return this.author;
    }

    public void setAuthor(String author) {
      this.author = author;
    }

    public int getYear() {
      return this.year;
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
      //System.out.println("-----Start Author Mapping with: "+value);

      StringTokenizer tokenizer = new StringTokenizer(value.toString());

      id.set(Integer.parseInt(tokenizer.nextToken()));

      StringBuilder author = new StringBuilder(tokenizer.nextToken());
      while(tokenizer.hasMoreTokens()){
        author.append(" ").append(tokenizer.nextToken());
      }

      output.setAuthor(author.toString());
      //System.out.println("ID: "+id+" Author: "+output.getAuthor());
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

      StringBuilder book = new StringBuilder(tokenizer.nextToken());
      while(tokenizer.hasMoreTokens()){
        book.append(" ").append(tokenizer.nextToken());
      }

      output.setBook(book.toString());
      output.setYear(year);

      //System.out.println("ID: "+id+" Book: "+book+" Year: "+year);
      context.write(id,output);
    }
  }

  public static class BookAuthorReducer extends Reducer<IntWritable,BookAuthorWritable,Text,NullWritable> {

    public void reduce(IntWritable id,Iterable<BookAuthorWritable> inputs,  Context context)
            throws IOException, InterruptedException {
      //System.out.println("-----Received Input: "+inputs + " with id: "+id);

      Iterator<BookAuthorWritable> it = inputs.iterator();
      List<String> cacheBook = new ArrayList<String>();
      List<Integer> cacheYear = new ArrayList<Integer>();
      Text author = new Text();

      while (it.hasNext()) {
        BookAuthorWritable value = it.next();
        //System.out.println("-----"+id+" Received Author: "+value.getAuthor() + " with book: "+value.getBook());
        if (!value.getAuthor().equals("NA")) {
          author.set(value.getAuthor());
        } else {
          cacheBook.add(value.getBook());
          cacheYear.add(value.getYear());
        }
      }

      for(int i = 0;i < cacheBook.size();i ++) {
        //System.out.println("-----Received Book: "+cacheBook.get(i) + " with id: "+id+" and year: "+cacheYear.get(i));
        Text textOut = new Text(author+"\t"+cacheBook.get(i)+"\t"+cacheYear.get(i));
        //System.out.println("Final Output: "+textOut);
        context.write(textOut,NullWritable.get());
      }
      }
    }
}