package SONMR;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
//import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
//import java.util.ArrayList;
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

            LinkedList<HashSet<Integer>> transactions = new LinkedList<HashSet<Integer>>();

            //HashSet<HashSet<Integer>> freq_itemsets = new HashSet<HashSet<Integer>>();

            LinkedHashMap<HashSet<Integer>, Integer> itemsets_support = new LinkedHashMap<HashSet<Integer>, Integer>(100);
            // HashSet<HashSet<Integer>> candidates = new HashSet<HashSet<Integer>>();

            // find support of one sets
            for(String transaction : value.toString().split("\n")){
                HashSet<Integer> new_transaction = new HashSet<Integer>(20);
                for (String item : transaction.split("\\s")) {
                    new_transaction.add(Integer.valueOf(item));

                    HashSet<Integer> itemset = new HashSet<Integer>();
                    itemset.add(Integer.valueOf(item));
                    itemsets_support.merge(itemset, 1, (a,b) -> a + b);
                }
                transactions.add(new_transaction);
            }
            
            int level = 1;
            LinkedHashSet<HashSet<Integer>> candidates;
            LinkedList<HashSet<Integer>> current_freq_sets;
            do {

                current_freq_sets = new LinkedList<HashSet<Integer>>();
                candidates = new LinkedHashSet<HashSet<Integer>>();
                // the following for each loop structure is from stack overflow:
                // https://stackoverflow.com/questions/4234985/how-to-for-each-the-hashmap
                for(Map.Entry<HashSet<Integer>, Integer> entry : itemsets_support.entrySet()) {
                    
                    HashSet<Integer> itemset = entry.getKey();
                    Integer support = entry.getValue();

                    if(support.intValue() >= thres){
                        for(HashSet<Integer> freq_itemset : current_freq_sets) {     
                                    
                            HashSet<Integer> new_candidate = new HashSet<Integer>();
                            new_candidate.addAll(itemset);
                            new_candidate.addAll(freq_itemset);
                            if(new_candidate.size() == level + 1){
                                candidates.add(new_candidate);
                            }
                        }   
                        current_freq_sets.add(itemset);
                        //freq_itemsets.add(itemset);
                        String toWrite = "";
                        for(Integer i : itemset){
                            toWrite += i;
                            toWrite += " ";
                        }
                        result.set(toWrite);
                        context.write(result, NullWritable.get());
                    } // if freq
                } // for 

                // add all the frequent itemsets to the HashSet
                // freq_itemsets.addAll(current_freq_sets);

                // candidates.clear();
                // for(HashSet<Integer> entry1 : current_freq_sets) {
                    
                //     for(HashSet<Integer> entry2 : current_freq_sets) {
                                
                //         if(!entry1.equals(entry2)){
                //             HashSet<Integer> new_candidate = new HashSet<Integer>();
                //             new_candidate.addAll(entry1);
                //             new_candidate.addAll(entry2);
                //             if(new_candidate.size() == level + 1){
                //                 candidates.add(new_candidate);
                //             }
                            
                //         } //generate candidates
                //     } //entry2
                // } //entry1
                
                // itemsets_support = new HashMap<HashSet<Integer>, Integer>();
                // for(String transaction : value.toString().split("\n")){
                //     for(HashSet<Integer> candidate : candidates){
                //         boolean itemset_contained = true;
                //         for(Integer i : candidate){
                            
                //             boolean contained = false;
                //             for (String item : transaction.split("\\s")) {
                //                 Integer temp = Integer.valueOf(item);
                //                 if(temp.equals(i)){
                //                     contained = true;
                //                     break;
                //                 }
                //             } // checking
                //             if(!contained){
                //                 itemset_contained = false;
                //                 break;
                //             }
                            
                //         } //item in itemset

                //         if(itemset_contained){
                //             itemsets_support.merge(candidate, 1, (a,b) -> a + b);
                //         }

                //     } //itemset
                // } //transaction

                itemsets_support = new LinkedHashMap<HashSet<Integer>, Integer>();
                for(HashSet<Integer> transaction : transactions){
                    for(HashSet<Integer> candidate : candidates){
                        if(transaction.containsAll(candidate)){
                            itemsets_support.merge(candidate, 1, (a,b) -> a + b);
                        }
                    }
                }

                level++;

            } while(!candidates.isEmpty()); // while{}

            
            // for(HashSet<Integer> freq_itemset : freq_itemsets){
            //     String toWrite = "";
            //     for(Integer i : freq_itemset){
            //         toWrite += i;
            //         toWrite += " ";
            //     }
            //     result.set(toWrite);
            //     context.write(result, NullWritable.get());
            // }

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

    public static class Mapper2
            extends Mapper<Object, Text, Text, IntWritable> {

        private final Text result = new Text();
        private final IntWritable itemset_support = new IntWritable();
        // To store the global shared variables.
        // private int dataset_size;
        // private int min_supp;
        private LinkedList<HashSet<Integer>> itemsets = new LinkedList<HashSet<Integer>>();

        
        public void setup(Context context) throws IOException{
            
            URI[] cacheFiles = context.getCacheFiles();
            
            BufferedReader readSet = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFiles[0].toString())));
            
            for (String itemset = readSet.readLine(); itemset != null; itemset = readSet.readLine()) {
                HashSet<Integer> new_itemset = new HashSet<Integer>();
                for(String item : itemset.split("\\s")){
                    new_itemset.add(Integer.valueOf(item));
                }
                itemsets.add(new_itemset);
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            

            LinkedList<HashSet<Integer>> transactions = new LinkedList<HashSet<Integer>>();
            for(String transaction : value.toString().split("\n")){
                HashSet<Integer> new_transaction = new HashSet<Integer>(20);
                for (String item : transaction.split("\\s")) {
                    new_transaction.add(Integer.valueOf(item));
                }
                transactions.add(new_transaction);
            }
            
            LinkedHashMap<HashSet<Integer>, Integer> itemsets_support = new LinkedHashMap<HashSet<Integer>, Integer>();
            itemsets_support = new LinkedHashMap<HashSet<Integer>, Integer>();
            for(HashSet<Integer> transaction : transactions){
                for(HashSet<Integer> itemset : itemsets){
                    if(transaction.containsAll(itemset)){
                        itemsets_support.merge(itemset, 1, (a,b) -> a + b);
                    }
                }
            }
            // for(String transaction : value.toString().split("\n")){
            //     for(HashSet<Integer> itemset : itemsets){
            //         boolean itemset_contained = true;
            //         for(Integer i : itemset){
                            
            //             boolean contained = false;
            //             for (String item : transaction.split("\\s")) {
            //                 Integer temp = Integer.valueOf(item);
            //                 if(temp.equals(i)){
            //                     contained = true;
            //                     break;
            //                 }
            //             } // checking
            //             if(!contained){
            //                 itemset_contained = false;
            //                 break;
            //             }
                            
            //         } //item in itemset

            //         if(itemset_contained){
            //             itemsets_support.merge(itemset, 1, (a,b) -> a + b);
            //         }
            //     } //itemset
            // } //transaction

            
            // WRITE OUT KEYS

            for(Map.Entry<HashSet<Integer>, Integer> entry : itemsets_support.entrySet()) {
                    
                HashSet<Integer> itemset = entry.getKey();
                String toWrite = "";
                for(Integer i : itemset){
                    toWrite += i;
                    toWrite += " ";
                }
                result.set(toWrite);
                
                Integer v = entry.getValue();
                itemset_support.set(v.intValue());
                
                context.write(result, itemset_support);

            }

        } // map()
    } // Mapper2 class

    public static class Reducer2
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable support = new IntWritable();
        // global variables
        int min_supp;

        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            // get the values of the global shared variables.
            min_supp = conf.getInt("min_supp", 0);
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(sum >= min_supp){
                support.set(sum);
                context.write(key, support);
            }
        }
    }

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

        job1.waitForCompletion(true);

        // Creating and setting up the second job. Must happen after setting the
        // global shared variables in the configuration
        Job job2 = Job.getInstance(conf, "sonmr");
        job2.setJarByClass(SONMR.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setInputFormatClass(MultiLineInputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        NLineInputFormat.setNumLinesPerSplit(job2, transactions_per_block);

        FileInputFormat.addInputPath(job2, new Path(args[4]));
        FileOutputFormat.setOutputPath(job2, new Path(args[6]));

        Path first_reducer_output = new Path(args[5] + "/part-r-00000");
        job2.addCacheFile(first_reducer_output.toUri());
        
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}