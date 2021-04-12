package SONMR;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;

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

    public static class Mapper1 extends Mapper<Object, Text, Text, NullWritable> {

        // create final instance variable to return keys with
        // value return is NullWritable so no varibale is necessary
        private final Text result = new Text();

        // to store the global shared variables.
        private int dataset_size;
        private int min_supp;
        private double corr_factor;
        private int transactions_per_block;

        // setup function to get global variables from config
        public void setup(Context context) throws IOException{
            // get configuration
            Configuration conf = context.getConfiguration();

            // get the values of the global shared variables.
            dataset_size = conf.getInt("dataset_size", Integer.MAX_VALUE);
            min_supp = conf.getInt("min_supp", 0);
            corr_factor = conf.getDouble("corr_factor", 1.0);
            transactions_per_block = conf.getInt("transactions_per_block", 100);

        }

        // map() function for Mapper1
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // calculate threshold for first mappers 
            double thres = corr_factor * ((double)min_supp / (double)dataset_size) * (double)transactions_per_block;

            int min_thres = (int) Math.ceil(thres);

            // frequent itemsets for pruning
            LinkedHashSet<HashSet<Integer>> all_freq_itemsets = new LinkedHashSet<HashSet<Integer>>();

            // create a LinkedList to store all the transactions as HashSets
            // this will make lookups much quicker in the next steps
            LinkedList<HashSet<Integer>> transactions = new LinkedList<HashSet<Integer>>();

            // create a LinkedHashMap that stores the support of itemsets
            // we use a Linked HashMap so we can have more efficient iteration
            // give it 1000 buckets, there shouldn't be much more than 1000 itemsets at each level
            // this saves time from rehashing when buckets are added 
            LinkedHashMap<HashSet<Integer>, Integer> itemsets_support = new LinkedHashMap<HashSet<Integer>, Integer>();

            // create a linkedList of the current frequent sets found when calculating supports
            // this helps create candidates
            LinkedList<HashSet<Integer>> current_freq_sets = new LinkedList<HashSet<Integer>>();

            // loop through transactions the first time
            // find supports of size 1 itemsets 
            // and put transactions into HashSets for easy contains() calls
            for(String transaction : value.toString().split("\n")){

                // create a HashSet to store this transaction
                HashSet<Integer> new_transaction = new HashSet<Integer>();

                // loop through items and update the support for that item
                // also add that item to the transaction HashSet
                for (String item : transaction.split("\\s")) {
                    
                    // add item to transaction
                    new_transaction.add(Integer.valueOf(item));

                    // put item into an itemset
                    HashSet<Integer> itemset = new HashSet<Integer>();
                    itemset.add(Integer.valueOf(item));

                    // if itemset is not in support HashMap, add with value 1
                    // otherwise increment value by 1
                    itemsets_support.merge(itemset, 1, (a,b) -> a + b);

                    // if itemset reaches min threshold:
                    // add it to frequent itemsets
                    // then write out key with NullWritable since it is a frequent itemset
                    if(itemsets_support.get(itemset) == min_thres){
                        current_freq_sets.add(itemset);
                        all_freq_itemsets.add(itemset);
                    
                        result.set(item);
                        context.write(result, NullWritable.get());
                    }
                }

                // add new transaction HashSet to LinkedList of transactions
                transactions.add(new_transaction);
            }

            // int to store size of itemsets we're processing
            int level = 1;
            // LinkedHashSet to store our candidates
            // HashSet removes duplicates
            // Linked allows it to be easily iterable
            LinkedHashSet<HashSet<Integer>> candidates;

            // do while loop that loops thru,
            // generates candidates,
            // then finds their support
            do {
                
                // initialize candidates LinkedHashSet
                candidates = new LinkedHashSet<HashSet<Integer>>();

                // generate candidates and prune from freq itemsets of size level-1
                // loop through all pairs of frequent sets to combine to form potential candidates
                for(HashSet<Integer> freq_set1 : current_freq_sets){
                    for(HashSet<Integer> freq_set2 : current_freq_sets){

                        // create a new HashSet to store our potential candidate
                        HashSet<Integer> new_candidate = new HashSet<Integer>();
                        // add all elements of the two freq sets
                        new_candidate.addAll(freq_set1);
                        new_candidate.addAll(freq_set2);
                        // make sure potential candidate is of level+1
                        if(new_candidate.size() == level + 1){
                            boolean toAdd = true;
                            // check to make sure all subsets of size-1 are frequent
                            for(Integer i : new_candidate){
                               HashSet<Integer> pruned = new HashSet<Integer>();
                                pruned.addAll(new_candidate);
                                pruned.remove(i);
                                    
                                if(!all_freq_itemsets.contains(pruned)){
                                    toAdd = false;
                                }
                            }
                            // if passed the pruning, add to candidates
                            if(toAdd){
                                candidates.add(new_candidate);
                            } // pruning
                                
                        } // candidate check
                    } // freq set 1 loop
                } // freq set 2 loop


                // initial new support and current freq sets data structures 
                // to clear them and store next round 
                itemsets_support = new LinkedHashMap<HashSet<Integer>, Integer>();
                current_freq_sets = new LinkedList<HashSet<Integer>>();

                // loop through all transactions and see if they contain the candidate itemsets
                for(HashSet<Integer> transaction : transactions){
                    // check to see if each candidate is in the transaction
                    for(HashSet<Integer> candidate : candidates){
                        // if candidate is in transaction, increment its support
                        if(transaction.containsAll(candidate)){
                            itemsets_support.merge(candidate, 1, (a,b) -> a + b);

                            // if support has gotten to min threshold:
                            // add it to the freq itemsets data structures
                            // and write out key with NullWritable
                            if(itemsets_support.get(candidate) == min_thres){
                                current_freq_sets.add(candidate);
                                all_freq_itemsets.add(candidate);
                                
                                String toWrite = "";
                                for(Integer i : candidate){
                                    toWrite += i;
                                    toWrite += " ";
                                }
                                result.set(toWrite);
                                context.write(result, NullWritable.get());
                                
                            } // if support surpasses min threshold
                        } // itemset in transaction check
                    } // loop thru candidates
                } // loop thru transactions

                // increment level
                level++;

            } while(!candidates.isEmpty()); // while{}

        } // map()
    } // Mapper1

    public static class Reducer1 extends Reducer<Text, NullWritable, Text, NullWritable> {

        // simply write key to output with a NullWritable for output
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {

        // store final instance variables for Text key ouput and IntWritable value output
        private final Text result = new Text();
        private final IntWritable itemset_support = new IntWritable();
        
        // create a LinkedList of LinkedHashSets to store itemsets from Mapper1 output
        private LinkedHashSet<LinkedHashSet<Integer>> itemsets = new LinkedHashSet<LinkedHashSet<Integer>>();

        // setup function to get cached file with Mapper1 output
        public void setup(Context context) throws IOException{
            
            // get cached files from config
            URI[] cacheFiles = context.getCacheFiles();
            
            // read in cached file
            BufferedReader readSet = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFiles[0].toString())));
            
            // loop thru file and put each itemset in a HashSet
            for (String itemset = readSet.readLine(); itemset != null; itemset = readSet.readLine()) {
                LinkedHashSet<Integer> new_itemset = new LinkedHashSet<Integer>();
                for(String item : itemset.split("\\s")){
                    new_itemset.add(Integer.valueOf(item));
                }
                itemsets.add(new_itemset);
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // create a LinkedList of LinkedHashSets to store transactions for quicker lookups later
            // same as Mapper1 semantics
            LinkedList<LinkedHashSet<Integer>> transactions = new LinkedList<LinkedHashSet<Integer>>();
            for(String transaction : value.toString().split("\n")){
                LinkedHashSet<Integer> new_transaction = new LinkedHashSet<Integer>();
                for (String item : transaction.split("\\s")) {
                    new_transaction.add(Integer.valueOf(item));
                }
                transactions.add(new_transaction);
            }
            
            // create a LinkedHashMap to get the itemsets supports
            LinkedHashMap<HashSet<Integer>, Integer> itemsets_support = new LinkedHashMap<HashSet<Integer>, Integer>();
            
            // loop thru all transactions and
            // if itemset is in that transaction,
            // incremenet its support
            for(HashSet<Integer> transaction : transactions){
                for(HashSet<Integer> itemset : itemsets){
                    if(transaction.containsAll(itemset)){
                        itemsets_support.merge(itemset, 1, (a,b) -> a + b);
                    }
                }
            }

            // loop through HashMap of support and output key value
            // pairs of (key, support)
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

    public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {

        // create final instance variable to store support of this key
        private final IntWritable support = new IntWritable();
        // to store global variables
        int min_supp;

        public void setup(Context context) throws IOException{
            // get config
            Configuration conf = context.getConfiguration();
            // get the values of the global shared variables.
            min_supp = conf.getInt("min_supp", 0);
        }

        // sum all the supports from mappers and return (key, support)
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
        
        // parse command line args
        int dataset_size = Integer.parseInt(args[0]);
        int transactions_per_block = Integer.parseInt(args[1]);
        int min_supp = Integer.parseInt(args[2]);
        double corr_factor = Double.parseDouble(args[3]);
               
        // create config
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
        
        double start_time = System.currentTimeMillis();

        boolean finished1 = job1.waitForCompletion(true);

        double mid_time = System.currentTimeMillis();
        System.out.println(mid_time - start_time);

        boolean finished2 = job2.waitForCompletion(true);
        
        double end_time = System.currentTimeMillis();
        System.out.println(end_time - mid_time);
        System.out.println(end_time - start_time);

        System.exit(finished1 && finished2 ? 0 : 1);
    }
}