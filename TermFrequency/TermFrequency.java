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


public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " termfrequency ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }

   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private boolean caseSensitive = false;
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      protected void setup(Context context)
        throws IOException,
          InterruptedException {
        Configuration config = context.getConfiguration();
        this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
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
}
