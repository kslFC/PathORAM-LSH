package com.pathoram;

public class Configs {
	//thread fixed number
	public static int THREAD_FIXED = 4;
	
	//server host name and port
	public static String SERVER_HOSTNAME = "localhost";
	public static int SERVER_PORT = 12339;
	
	// Path ORAM tree: a full binary tree with 31 buckets and 16 leaves.
	public static int BUCKET_COUNT = 31;//total bucket stored in the server
	public static int LEAF_COUNT = (BUCKET_COUNT+1)/2;//total leaf count
	public static int LEAF_START = BUCKET_COUNT - LEAF_COUNT;//leaf start index in tree node(root is 0)
	public static int Z = 4;//block count in every bucket
	public static int BLOCK_COUNT = 48;//logical blocks loaded from data/0.bin through data/47.bin
	public static int HEIGHT = (int) (Math.log(BUCKET_COUNT)/Math.log(2) + 1);//tree height
	public static int BLOCK_DATA_LEN = 1392;//348 float values, matching each Roost ORAM data file
	// Client-side READ cache. Cached blocks remain in the ORAM stash until path eviction.
	public static boolean READ_CACHE_ENABLED = true;
	// Delayed single-block eviction settings, following Roost ORAM's policy.
	public static int DEFAULT_DELAYED_EVICT_COUNT = 4;
	public static int HARD_STASH_LIMIT = 36;
	public static int MAX_SINGLE_EVICT_PER_ACCESS = 10;
	// Roost-compatible LSH: hash all blocks, cluster by Hamming distance, and select a medoid leaf.
	public static int LSH_HASH_BITS = 256;
	public static long LSH_SEED = 42L;
	
	//storage file directory
	public static String STORAGE_PATH = System.getProperty(
			"pathoram.storage", "/tmp/serverStorage/bucket_");
	
	
	
}
