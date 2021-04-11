package SONMR;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Map;
//import java.util.AbstractMap;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

public class SONMR {

    public static class Mapper1
            extends Mapper<Object, Text, Text, NullWritable> {

        private final Text result = new Text();
        // To store the global shared variables.
        private int dataset_size;
        private int min_supp;
        private double corr_factor;
        private int transactions_per_block;

        // This method is new (not in WordCount). We use it to read the
        // global shared variables and to populate the set containing the
        // stopwords using the file in the distributed cache.
        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            // Get the values of the global shared variables.
            dataset_size = conf.getInt("dataset_size", Integer.MAX_VALUE);
            min_supp = conf.getInt("min_supp", 0);
            corr_factor = conf.getDouble("corr_factor", 1.0);
            transactions_per_block = conf.getInt("transactions_per_block", 100);

        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            double thres = corr_factor * ((double)min_supp / (double)dataset_size) * (double)transactions_per_block;
            System.err.println("THRESHOLD:" + thres);

            HashSet<HashSet<Integer>> freq_itemsets = new HashSet<HashSet<Integer>>();

            HashMap<HashSet<Integer>, Integer> itemsets_support = new HashMap<HashSet<Integer>, Integer>();
            HashSet<HashSet<Integer>> candidates = new HashSet<HashSet<Integer>>();

            // find support of one sets
            for(String transaction : value.toString().split("\n")){
                for (String item : transaction.split("\\s")) {
                    HashSet<Integer> itemset = new HashSet<Integer>();
                    itemset.add(Integer.valueOf(item));
                    itemsets_support.merge(itemset, 1, (a,b) -> a + b);
                }
            }
            
            int level = 1;
            do {

                HashSet<HashSet<Integer>> current_freq_sets = new HashSet<HashSet<Integer>>();
                
                // the following for each loop structure is from stack overflow:
                // https://stackoverflow.com/questions/4234985/how-to-for-each-the-hashmap
                for(Map.Entry<HashSet<Integer>, Integer> entry : itemsets_support.entrySet()) {
                    
                    HashSet<Integer> itemset = entry.getKey();
                    Integer v = entry.getValue();

                    System.err.print(itemset);
                    System.err.println(" : " + v);
                    if(v.intValue() >= thres){
                        current_freq_sets.add(itemset);
                    }
                }

                // add all the frequent itemsets to the HashSet
                freq_itemsets.addAll(current_freq_sets);

                candidates.clear();
                for(HashSet<Integer> entry1 : current_freq_sets) {
                    
                    for(HashSet<Integer> entry2 : current_freq_sets) {
                                
                        if(!entry1.equals(entry2)){
                            HashSet<Integer> new_candidate = new HashSet<Integer>();
                            new_candidate.addAll(entry1);
                            new_candidate.addAll(entry2);
                            if(new_candidate.size() == level + 1){
                                candidates.add(new_candidate);
                            }
                            
                        } //generate candidates
                    } //entry2
                } //entry1
                
                itemsets_support.clear();
                for(String transaction : value.toString().split("\n")){
                    for(HashSet<Integer> candidate : candidates){
                        boolean itemset_contained = true;
                        for(Integer i : candidate){
                            
                            boolean contained = false;
                            for (String item : transaction.split("\\s")) {
                                Integer temp = Integer.valueOf(item);
                                if(temp.equals(i)){
                                    contained = true;
                                    break;
                                }
                            } // checking
                            if(!contained){
                                itemset_contained = false;
                                break;
                            }
                            
                        } //item in itemset

                        if(itemset_contained){
                            itemsets_support.merge(candidate, 1, (a,b) -> a + b);
                        }

                    } //itemset
                } //transaction

                level++;

            } while(!candidates.isEmpty()); // while{}

            
            // WRITE OUT KEYS
            // String toWrite = "";
            // for(HashSet<Integer> freq_itemset : freq_itemsets){
            //     for(Integer i : freq_itemset){
            //         toWrite += i;
            //         toWrite += " ";
            //     }
            //     toWrite += "\n";
            // }

            // //System.out.println("THIS IS TOWRITE: " + toWrite);
            
            // result.set(toWrite);
            // context.write(result, NullWritable.get());

            
            for(HashSet<Integer> freq_itemset : freq_itemsets){
                String toWrite = "";
                for(Integer i : freq_itemset){
                    toWrite += i;
                    toWrite += " ";
                }
                result.set(toWrite);
                context.write(result, NullWritable.get());
            }

        } // map()
    } // Mapper1

    public static class Reducer1
            extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    // public static class Mapper2
    //         extends Mapper<Object, Text, Text, IntWritable> {

    //     private final Text result = new Text();
    //     private final IntWritable final_support = new IntWritable();
    //     // To store the global shared variables.
    //     private int dataset_size;
    //     private int min_supp;
    //     private HashSet<HashSet<Integer>> itemsets = new HashSet<HashSet<Integer>>();

    //     // This method is new (not in WordCount). We use it to read the
    //     // global shared variables and to populate the set containing the
    //     // stopwords using the file in the distributed cache.
    //     public void setup(Context context) throws IOException{
    //         Configuration conf = context.getConfiguration();
    //         // Get the values of the global shared variables.
    //         dataset_size = conf.getInt("dataset_size", Integer.MAX_VALUE);
    //         min_supp = conf.getInt("min_supp", 0);

    //         // Get the list of files in the distributed cache
    //         URI[] cacheFiles = context.getCacheFiles();
    //         // Create a reader for the only file in the cache, which is the
    //         //stopwords.txt file.
    //         BufferedReader readSet = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFiles[0].toString())));
    //         // Read in the file and populate the HashSet
    //         for (String itemset = readSet.readLine(); itemset != null; itemset = readSet.readLine()) {
    //             HashSet<Integer> new_itemset = new HashSet<Integer>();
    //             for(String item : itemset.split("\\s")){
    //                 new_itemset.add(Integer.valueOf(item));
    //             }
    //             itemsets.add(new_itemset);
    //         }
    //     }

    //     public void map(Object key, Text value, Context context
    //     ) throws IOException, InterruptedException {

    //         HashMap<HashSet<Integer>, Integer> itemsets_support = new HashMap<HashSet<Integer>, Integer>();
            
    //         for(String transaction : value.toString().split("\n")){
    //             for(HashSet<Integer> itemset : itemsets){
    //                 boolean itemset_contained = true;
    //                 for(Integer i : itemset){
                            
    //                     boolean contained = false;
    //                     for (String item : transaction.split("\\s")) {
    //                         Integer temp = Integer.valueOf(item);
    //                         if(temp.equals(i)){
    //                             contained = true;
    //                             break;
    //                         }
    //                     } // checking
    //                     if(!contained){
    //                         itemset_contained = false;
    //                         break;
    //                     }
                            
    //                 } //item in itemset

    //                 if(itemset_contained){
    //                     itemsets_support.merge(itemset, 1, (a,b) -> a + b);
    //                 }
    //             } //itemset
    //         } //transaction

            
    //         // WRITE OUT KEYS
    //         String toWrite = "";
    //         for(HashSet<Integer> itemset : itemsets){
    //             for(Integer i : itemset){
    //                 toWrite += i;
    //                 toWrite += " ";
    //             }
    //             toWrite += "\n";
    //         }

    //         //System.out.println("THIS IS TOWRITE: " + toWrite);
            
    //         result.set(toWrite);
    //         context.write(result, NullWritable.get());

    //     } // map()
    // } // Mapper2 class

    // public static class Reducer2
    //         extends Reducer<Text, NullWritable, Text, NullWritable> {

    //     public void reduce(Text key, Iterable<NullWritable> values,
    //                        Context context
    //     ) throws IOException, InterruptedException {
    //         context.write(key, NullWritable.get());
    //     }
    // }

    public static void main(String[] args) throws Exception {
        
        int dataset_size = Integer.parseInt(args[0]);
        int transactions_per_block = Integer.parseInt(args[1]);
        int min_supp = Integer.parseInt(args[2]);
        double corr_factor = Double.parseDouble(args[3]);
               
        Configuration conf = new Configuration();
        // Setting the global shared variables in the configuration
        conf.setInt("dataset_size", dataset_size);
        conf.setInt("transactions_per_block", transactions_per_block);
        conf.setInt("min_supp", min_supp);
        conf.setDouble("corr_factor", corr_factor);
        
        // Creating and setting up the first job. Must happen after setting the
        // global shared variables in the configuration
        Job job1 = Job.getInstance(conf, "sonmr");
        job1.setJarByClass(SONMR.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setInputFormatClass(MultiLineInputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        NLineInputFormat.setNumLinesPerSplit(job1, transactions_per_block);

        FileInputFormat.addInputPath(job1, new Path(args[4]));
        FileOutputFormat.setOutputPath(job1, new Path(args[5]));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);

        // Creating and setting up the second job. Must happen after setting the
        // global shared variables in the configuration
        // Job job2 = Job.getInstance(conf, "sonmr");
        // job2.setJarByClass(SONMR.class);
        // job2.setMapperClass(Mapper2.class);
        // job2.setReducerClass(Reducer2.class);
        // job2.setInputFormatClass(MultiLineInputFormat.class);
        // job2.setOutputKeyClass(Text.class);
        // job2.setOutputValueClass(IntWritable.class);
        // NLineInputFormat.setNumLinesPerSplit(job2, transactions_per_block);

        // FileInputFormat.addInputPath(job2, new Path(args[4]));
        // FileOutputFormat.setOutputPath(job2, new Path(args[6]));

        // Path first_reducer_output = new Path(args[5] + "/part-r-00000");
        // job2.addCacheFile(first_reducer_output.toUri());
        
        // System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}