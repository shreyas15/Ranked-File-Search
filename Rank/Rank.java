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


public class Rank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Rank.class);
   public static long fileCount = 0;

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Rank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {

     // making new file names
     String tempPath = args[1] + "temp";
     String tempPath2 = args[1] + "temp2";
     String tempPath3 = args[1] + "temp3";

     //Job 1

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

      //Find number of arguments apart from the input and output paths
      String[] numArgs = new String[args.length - 2];
		  for(int i = 2; i < args.length; i++){
			     numArgs[i-2] = args[i];
      }
      if (numArgs.length == 0)
        return 1;

      job1.waitForCompletion(true);

      //Job 2

      Job job2  = Job .getInstance(getConf(), " tfidf ");
      job2.setJarByClass( this .getClass());

      FileInputFormat.addInputPath(job2, new Path(tempPath));
      FileOutputFormat.setOutputPath(job2,  new Path(tempPath2));
      job2.setMapperClass( Mapidf .class);
      job2.setReducerClass( Reduceidf .class);
      job2.setOutputKeyClass( Text .class);
      job2.setOutputValueClass( Text .class);
      Configuration confB = job2.getConfiguration();
      confB.setLong("filecount",fileCount);
      job2.waitForCompletion(true);

      //Job 3

      Job job3  = Job .getInstance(getConf(), " search ");
      job3.setJarByClass( this .getClass());

      FileInputFormat.addInputPath(job3, new Path(tempPath2));
      FileOutputFormat.setOutputPath(job3,  new Path(tempPath3));
      job3.setMapperClass( MapSearch .class);
      job3.setReducerClass( ReduceSearch .class);
      job3.setOutputKeyClass( Text .class);
      job3.setOutputValueClass( Text .class);
      Configuration confC = job3.getConfiguration();
      confC.setStrings("argCount",numArgs);

      job3.waitForCompletion(true);

      //Job 4

      Job job4  = Job .getInstance(getConf(), " rank ");
      job4.setJarByClass( this .getClass());

      FileInputFormat.addInputPath(job4, new Path(tempPath3));
      FileOutputFormat.setOutputPath(job4,  new Path(args[1]));
      job4.setMapperClass( MapRank .class);
      job4.setReducerClass( ReduceRank .class);
      job4.setOutputKeyClass( FloatWritable .class);
      job4.setOutputValueClass( Text .class);

      return job4.waitForCompletion(true)  ? 0 : 1;
   }
   public static class MapRank extends Mapper<LongWritable ,  Text ,  FloatWritable ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private boolean caseSensitive = true;
      private static final Pattern RANK_BOUNDARY = Pattern .compile("\\s*\\b\\.\\b\\t\\b\\.\\b\\s*");

      // protected void setup(Context context)
      //   throws IOException,
      //     InterruptedException {
      //   Configuration config = context.getConfiguration();
      //   this.caseSensitive = config.getBoolean("wordcount.case.sensitive", true);
      // }

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();

         // check for case sensitivity.
         Text currentWord  = new Text();
         FloatWritable currentValue = new FloatWritable();
         String[] partsA = new String[2];

        //  if (!caseSensitive) {
        //    line = line.toLowerCase();
        //  }
         for ( String word  : RANK_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            partsA = word.trim().split("\\t");
            // partA[0] has filename
            // partA[1] has score

            currentWord = new Text(partsA[0].trim());
            float tempfl = (float)(-1.0) * Float.parseFloat(partsA[1].trim());
            currentValue = new FloatWritable(tempfl);
              //by making the value negative and using as key since the values are sorted using this, we reverse the sorting.
            context.write(currentValue, currentWord);
         }
      }
   }

   public static class ReduceRank extends Reducer<FloatWritable ,  Text ,  Text ,  FloatWritable > {
      @Override
      public void reduce( FloatWritable nowValue,  Iterable<Text>  word,  Context context)
         throws IOException,  InterruptedException {

          //Extracting content from words to count files in case values are the same
         ArrayList<String> wordCache = new ArrayList<String>();

         float negate  = 0;
         negate = (float)(-1.0) * nowValue.get();
         //Iterator<Text> it = word.iterator();

         String writer = new String();
         //counter multiplying by -1 to retrieve original value
         for ( Text count  : word) {
           writer = count.toString().trim();
           wordCache.add(writer);
         }
         for (int i = 0; i < wordCache.size(); i++){
           context.write(new Text(wordCache.get(i)), new FloatWritable(negate));
         }
      }
   }

   public static class MapSearch extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private boolean caseSensitive = true;
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\b\\#####\b\\.\\b\\t\\b\\.\\b\\s*");

      // protected void setup(Context context)
      //   throws IOException,
      //     InterruptedException {
      //   Configuration config = context.getConfiguration();
      //   this.caseSensitive = config.getBoolean("wordcount.case.sensitive", true);
      // }

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         //Fetching number of arguments from current context.
         Configuration argsRead = context.getConfiguration();
         String[] numArgs2 = argsRead.getStrings("argCount");

         // check for case sensitivity.

        //  if (!caseSensitive) {
        //    line = line.toLowerCase();
        //  }
         Text currentWord  = new Text();
         Text currentValue = new Text();
         String[] partsA = new String[2];
         String[] partsB = new String[2];

         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            partsA = word.split("#####");
            partsB = partsA[1].split("  ");
            // partA[0] has word
            // partB[0] has filename
            // partB[1] has WF

            if (Arrays.asList(numArgs2).contains(partsA[0]))
            {
              currentWord = new Text(partsB[0]);
              currentValue = new Text(partsB[1].trim());
              context.write(currentWord, currentValue);
            }
            else
              continue;
         }
      }
   }

   public static class ReduceSearch extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override
      public void reduce( Text word,  Iterable<Text > counts,  Context context)
         throws IOException,  InterruptedException {
         float sum  = 0;
         for ( Text count  : counts) {
            sum  += Float.parseFloat(count.toString());
         }
         Text nowValue = new Text(Float.toString(sum));

         context.write(word, nowValue);
      }
   }

   public static class Mapidf extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private boolean caseSensitive = true;
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\b\\#####\b\\.\\b\\t\\b\\.\\b\\s*");

      // protected void setup(Context context)
      //   throws IOException,
      //     InterruptedException {
      //   Configuration config = context.getConfiguration();
      //   this.caseSensitive = config.getBoolean("wordcount.case.sensitive", true);
      // }

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();

         // check for case sensitivity.

        //  if (!caseSensitive) {
        //    line = line.toLowerCase();
        //  }
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

   public static class Reduceidf extends Reducer<Text ,  Text ,  Text ,  Text > {

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
           context.write(new Text(newWord2),  new Text(TFIDF + ""));
           //context.write(new Text(word),  new Text(valCache.get(i)));
         }
      }
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

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  Text > {
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
         context.write(word,  new Text(wordFreq+""));
      }
   }
}
