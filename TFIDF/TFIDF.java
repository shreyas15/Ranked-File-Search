// Author: Shreyas Subramanya Bhat
// Data: 02/16/2017
// Email: ssubra15@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// additional libraries imported
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import java.lang.Math;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.mapred.JobConf;
import java.util.*;


public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);
   public static long fileCount = 0;

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TFIDF(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {

     //Job 1
      String tempPath = args[1] + "temp";
      Job job1  = Job .getInstance(getConf(), " termfrequency ");
      job1.setJarByClass( this .getClass());

      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1,  new Path(tempPath));
      job1.setMapperClass( Map .class);
      job1.setReducerClass( Reduce .class);
      job1.setOutputKeyClass( Text .class);
      job1.setOutputValueClass( IntWritable .class);

      // //finding input file count

      Configuration confA = job1.getConfiguration();
      FileSystem fs = FileSystem.get(confA);
      Path inPath = new Path(args[0]);
      ContentSummary cs = fs.getContentSummary(inPath);
      fileCount = cs.getFileCount();

      job1.waitForCompletion(true);

      //Job 2

      Job job2  = Job .getInstance(getConf(), " tfidf ");
      job2.setJarByClass( this .getClass());

      FileInputFormat.addInputPath(job2, new Path(tempPath));
      FileOutputFormat.setOutputPath(job2,  new Path(args[1]));
      job2.setMapperClass( Mapidf .class);
      job2.setReducerClass( Reduceidf .class);
      job2.setOutputKeyClass( Text .class);
      job2.setOutputValueClass( Text .class);
      Configuration confB = job2.getConfiguration();
      confB.setLong("filecount",fileCount);

      return job2.waitForCompletion(true)  ? 0 : 1;
   }

   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private boolean caseSensitive = true;
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      protected void setup(Context context)
        throws IOException,
          InterruptedException {
        Configuration config = context.getConfiguration();
        this.caseSensitive = config.getBoolean("wordcount.case.sensitive", true);
      }

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();

         // check for case sensitivity.

         if (!caseSensitive) {
           line = line.toLowerCase();
         }
         Text currentWord  = new Text();
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }

            //getting filename using inputSplit and making a new key with ##### delimiter

            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String newWord = word.trim() + "#####" + fileName + "  ";
            currentWord  = new Text(newWord);
            context.write(currentWord,one);
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  FloatWritable > {
      @Override
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }

         //Calculating Word Frequency using formula #1

         float wordFreq = 0;
         if (sum > 0)
            wordFreq =1 + (float)Math.log10(sum);
         else
            wordFreq = 0;
         context.write(word,  new FloatWritable(wordFreq));
      }
   }
   public static class Mapidf extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private boolean caseSensitive = true;
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\b\\#####\b\\.\\b\\t\\b\\.\\b\\s*");

      protected void setup(Context context)
        throws IOException,
          InterruptedException {
        Configuration config = context.getConfiguration();
        this.caseSensitive = config.getBoolean("wordcount.case.sensitive", true);
      }

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();

         // check for case sensitivity.

         if (!caseSensitive) {
           line = line.toLowerCase();
         }
         String[] partsA = new String[2];
         String[] partsB = new String[2];
         Text currentWord  = new Text();
         Text currentValue  = new Text();
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }

            //extract parts by separating delimiters

            partsA = word.split("#####");
            partsB = partsA[1].split("  ");
            // partA[0] has word
            // partB[0] has filename
            // partB[1] has WF

            currentWord  = new Text(partsA[0].trim());
            currentValue  = new Text(partsB[0].trim() + "=" + partsB[1].trim());
            context.write(currentWord,currentValue);
         }
      }
   }

   public static class Reduceidf extends Reducer<Text ,  Text ,  Text ,  FloatWritable > {

     private static final Pattern WORD_BOUNDARY2 = Pattern .compile("\\s*\\b\\.\\b\\=\\b\\.\\b\\s*");

      @Override
      public void reduce( Text word,  Iterable<Text> values,  Context context)
         throws IOException,  InterruptedException {

         //Fetching number of documents from current context.
         Configuration confRead = context.getConfiguration();
         long filecount = Long.parseLong(confRead.get("filecount"));

         float IDF = 0;
         float TFIDF = 0;

         //Extracting content from values
         ArrayList<String> valCache = new ArrayList<String>();

         String temp1;
         String[] temp2 = new String[2];
         long docCount  = 0;

         for (Text valueExt : values) {
           valCache.add(valueExt.toString()); //[0] has file name and [1] has WF
           docCount++; //number of docs having this particular word.
         }

         for (int i = 0; i < valCache.size(); i++){

           //Calculating IDF using formula #3
           IDF = (float)Math.log10(1 + (filecount/docCount));
           temp1 = (valCache.get(i)).toString();
           temp2 = temp1.trim().split("=");

           //Calculating TFIDF using formula #4

           TFIDF = IDF * Float.parseFloat(temp2[1].trim());
           String newWord2 = word + "#####" + temp2[0].trim() + "  ";
           context.write(new Text(newWord2),  new FloatWritable(TFIDF));
           //context.write(new Text(word),  new Text(valCache.get(i)));
         }
      }
   }
}
