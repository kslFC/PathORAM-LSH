package com.client;

/*
 * client connects to the server
 * initializes server
 * gets path from the server, finds the block he wants
 * writes back path to the server
 */
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.security.SecureRandom;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.example.LSH;
import com.pathoram.*;

public class Client implements ClientInterface{
	
	public static int requestID = 0;
	private int readCount;
	private int cacheHitCount;
	private int serverReadCount;
	private int singleEvictAttempts;
	private int singleEvictSuccesses;
	private int fallbackPathEvictCount;
	private int fallbackEvictCursor;
	private int lshAssignmentCount;
	private int lshClusterCount;
	
	protected InetSocketAddress serverAddress;
	protected AsynchronousChannelGroup mThreadGroup;
	protected AsynchronousSocketChannel mChannel;
	
	int[] position_map;
	Stash stash;
	RandomForORAM rand;
	ByteSerializable seria;
	private final boolean useReadCache;
	private final boolean useLshLeafAssignment;
	private final LSH lsh;
	private final String[] lshHashes;
	private String[] lshLeafMedoids;
	private int[] lshClusterLeafStarts;
	private int[] lshClusterLeafCounts;
	private int lshClusterPrefixBits;
	
	@SuppressWarnings("rawtypes")
	public Client(){
		this(Configs.READ_CACHE_ENABLED);
	}

	@SuppressWarnings("rawtypes")
	public Client(boolean useReadCache){
		this(useReadCache, new SecureRandom().nextLong());
	}

	@SuppressWarnings("rawtypes")
	public Client(boolean useReadCache, long oramSeed){
		this(useReadCache, false, oramSeed);
	}

	public Client(boolean useReadCache, boolean useLshLeafAssignment, long oramSeed){
		this.useReadCache = useReadCache;
		this.useLshLeafAssignment = useLshLeafAssignment;
		this.lsh = useLshLeafAssignment
				? new LSH(Configs.LSH_HASH_BITS, Configs.BLOCK_DATA_LEN / Float.BYTES, Configs.LSH_SEED)
				: null;
		this.lshHashes = useLshLeafAssignment ? new String[Configs.BLOCK_COUNT] : null;
		this.lshLeafMedoids = null;
		this.lshClusterLeafStarts = null;
		this.lshClusterLeafCounts = null;
		this.lshClusterPrefixBits = 0;
		this.position_map = new int[Configs.BLOCK_COUNT];
		this.stash = new Stash();
		this.rand = new RandomForORAM(oramSeed);
		this.seria = new ByteSerializable();
		//give each block a initialize random path
		for(int i=0;i<Configs.BLOCK_COUNT;i++){
			position_map[i] = rand.getRandomLeaf() + Configs.LEAF_START;
		}
		//connect to server
		try {
			serverAddress = new InetSocketAddress(Configs.SERVER_HOSTNAME,Configs.SERVER_PORT);
			mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(Configs.THREAD_FIXED, Executors.defaultThreadFactory());
			mChannel = AsynchronousSocketChannel.open(mThreadGroup);
			Future connection = mChannel.connect(serverAddress);
			connection.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void initServer() {
		// TODO Auto-generated method stub
		ByteBuffer header = MessageUtility.createMessageHeaderBuffer(MessageUtility.ORAM_INIT, 0);
		byte[] responseBytes = sendAndGetMessage(header, MessageUtility.ORAM_INIT);
		responseBytes = null;
	}

	public byte[] obliviousAccess(int blockIndex, byte[] newData, Operation op){
		requestID++;
		
		byte[] returndata;// return data
		boolean cacheHit = false;
		if (op == Operation.READ) {
			readCount++;
		}
		
		//get path id
		int position = position_map[blockIndex];
		// Baseline and initial writes use fresh random leaves. LSH modes choose the
		// next leaf after the READ data is available.
		if (!useLshLeafAssignment || op == Operation.WRITE) {
			position_map[blockIndex] = rand.getRandomLeaf() + Configs.LEAF_START;
		}
		// A cache hit means the newest copy is already in the client stash. Keep it
		// there and skip path I/O; changing the position map makes any old server
		// copy unreachable on subsequent accesses. Later path/single-block eviction
		// can place the cached copy at its current assigned leaf.
		Block b = op == Operation.READ && useReadCache
				? stash.find_by_index(blockIndex) : null;
		if (b != null) {
			cacheHit = true;
			cacheHitCount++;
		} else {
			if (op == Operation.READ) {
				serverReadCount++;
			}
			//read path (all writes and cache misses preserve baseline behavior)
			readPath(position);
			//get request block from stash
			b = stash.find_by_index(blockIndex);
		}
		
		//write request
		if(op == Operation.WRITE){
			b = new Block(position_map[blockIndex],blockIndex,newData);
			stash.add(b);//update block path id and data
			returndata = newData;
		}
		//read request
		else{
			if(b == null){//didn't find block in stash, return 0
				returndata = new byte[Configs.BLOCK_DATA_LEN];
			}else{
				returndata = b.getData();
				if (useLshLeafAssignment) {
					int assignedLeaf = chooseLeafFromHash(hashForBlock(blockIndex, returndata));
					position_map[blockIndex] = assignedLeaf;
					lshAssignmentCount++;
					logLshRemap(blockIndex, position, assignedLeaf,
							hashForBlock(blockIndex, returndata));
				}
				b.setLeaf_id(position_map[blockIndex]);
				stash.updataBlock(b);//update block path id
			}
		}
		// Cache hits do not read the path, so writing a path based on stash alone
		// would overwrite blocks that were already on the server. Keep those blocks
		// cached until a future READ miss provides the complete path to rebuild.
		if (cacheHit) {
			delayed_single_evict_after_access(blockIndex, useLshLeafAssignment);
			if (op == Operation.READ) {
				logStashSnapshot("AFTER-READ", "read=" + readCount + " block=" + blockIndex);
			}
			return returndata;
		}
		// get blocks from client stash to proper bucket
		Bucket[] bucket_list;
		if (useLshLeafAssignment && op == Operation.READ) {
			// Prefer every eligible shared ancestor above the subcluster; if those
			// buckets are full, continue from the subcluster root down to the leaf.
			bucket_list = buildLshPathFromStash(position, blockIndex, true);
		} else {
			bucket_list = new Bucket[Configs.HEIGHT];
			int countPtr = 0;
			for (int pos_run = position; pos_run >= 0; pos_run = (pos_run - 1) >> 1) {
				int retainedCacheBlock = useReadCache && op == Operation.READ ? blockIndex : -1;
				Block[] block_list = useReadCache && op == Operation.READ
						? stash.remove_expired_by_bucket(pos_run, retainedCacheBlock)
						: stash.remove_by_bucket(pos_run, -1, retainedCacheBlock);
				ArrayList<Block> blocks = new ArrayList<Block>(Configs.Z);
				for (int i = 0; i < Configs.Z; i++) {
					blocks.add(block_list[i]);
				}
				bucket_list[countPtr++] = new Bucket(blocks);
				if (pos_run == 0) break;
			}
		}
		
		//write path
		writePath(position, bucket_list);
		bucket_list = null;
		if (op == Operation.READ && (useReadCache || useLshLeafAssignment)) {
			if (useLshLeafAssignment) {
				delayed_single_evict_after_access(blockIndex, true);
			} else {
				delayed_single_evict_after_access(blockIndex, false);
			}
		}
		if (op == Operation.READ) {
			logStashSnapshot("AFTER-READ", "read=" + readCount + " block=" + blockIndex);
		}
		
		return returndata;
	}

	public int getReadCount() {
		return readCount;
	}

	public int getCacheHitCount() {
		return cacheHitCount;
	}

	public int getServerReadCount() {
		return serverReadCount;
	}

	public double getCacheHitRate() {
		return readCount == 0 ? 0.0 : (double) cacheHitCount / readCount;
	}

	public int getSingleEvictAttempts() {
		return singleEvictAttempts;
	}

	public int getSingleEvictSuccesses() {
		return singleEvictSuccesses;
	}

	public int getFallbackPathEvictCount() {
		return fallbackPathEvictCount;
	}

	public int getLshAssignmentCount() {
		return lshAssignmentCount;
	}

	public int getLshClusterCount() {
		return lshClusterCount;
	}

	/** Age stash blocks after each cached READ and evict expired blocks immediately. */
	private void delayed_single_evict_after_access(int accessedBlockIndex, boolean targetOnly) {
		for (Block block : stash.blocksSnapshot()) {
			if (block.getIndex() != accessedBlockIndex) {
				block.decrementEvictCount();
			}
		}

		Set<Integer> attempted = new HashSet<Integer>();
		int evictionsThisAccess = 0;
		while (evictionsThisAccess < Configs.MAX_SINGLE_EVICT_PER_ACCESS) {
			Block candidate = selectDelayedEvictCandidate(accessedBlockIndex, attempted);
			if (candidate == null) {
				break;
			}
			attempted.add(candidate.getIndex());
			singleEvictAttempts++;
			if (single_evict_block(candidate, targetOnly)) {
				singleEvictSuccesses++;
			} else {
				// Retain it for later retry if its current path has no free bucket slot.
				candidate.setEvictCount(0);
			}
			evictionsThisAccess++;
		}

		// Retain Roost's emergency path-eviction fallback for unusually large stashes.
		if (stash.size() > Configs.HARD_STASH_LIMIT) {
			int leaf = Configs.LEAF_START + (fallbackEvictCursor % Configs.LEAF_COUNT);
			fallbackEvictCursor = (fallbackEvictCursor + 1) % Configs.LEAF_COUNT;
			readPath(leaf);
			if (useLshLeafAssignment) {
				writePath(leaf, buildLshPathFromStash(leaf, -1, false));
			} else {
				writePathFromStash(leaf);
			}
			fallbackPathEvictCount++;
		}
	}

	private Block selectDelayedEvictCandidate(int accessedBlockIndex, Set<Integer> attempted) {
		Block oldest = null;
		for (Block block : stash.blocksSnapshot()) {
			int index = block.getIndex();
			if (index == accessedBlockIndex || attempted.contains(index)
					|| block.getEvictCount() > 0) {
				continue;
			}
			if (oldest == null || block.getEvictCount() < oldest.getEvictCount()) {
				oldest = block;
			}
		}
		return oldest;
	}

	/** Evict the expired block and its peers assigned to the exact same leaf path. */
	private boolean single_evict_block(Block target, boolean targetOnly) {
		if (target == null) {
			return false;
		}
		int leaf = target.getLeaf_id();
		if (leaf < Configs.LEAF_START || leaf >= Configs.BUCKET_COUNT) {
			return false;
		}
		if (targetOnly) {
			return single_evict_target_only(target, leaf);
		}

		readPath(leaf);
		logStashSnapshot("BEFORE-SINGLE-EVICT-WRITE", "targetLeaf=" + leaf);
		int stashSizeBeforePathWrite = stash.size();
		writePathFromStashForLeaf(leaf, target.getIndex());
		logStashSnapshot("AFTER-SINGLE-EVICT-WRITE", "targetLeaf=" + leaf);
		int blocksWrittenOnPath = stashSizeBeforePathWrite - stash.size();
		boolean evicted = stash.find_by_index(target.getIndex()) == null;
		if (evicted) {
			System.out.println("[SINGLE-EVICT] blockIndex=" + target.getIndex()
					+ " path=" + leaf
					+ " blocksWrittenOnPath=" + blocksWrittenOnPath
					+ " stashSize=" + stash.size());
			return true;
		}
		System.out.println("[SINGLE-EVICT-FAIL] blockIndex=" + target.getIndex()
				+ " path=" + leaf + " stashSize=" + stash.size());
		return false;
	}

	/** Roost-style LSH eviction: move only the selected stash block into one free bucket slot. */
	private boolean single_evict_target_only(Block target, int leaf) {
		for (int bucketId : pathFromRootToLeaf(leaf)) {
			Bucket bucket = readBucket(bucketId);
			if (bucket == null) {
				return false;
			}
			for (Block slot : bucket.getBlocks()) {
				if (slot.getIndex() == target.getIndex()) {
					slot.setLeaf_id(leaf);
					slot.setData(target.getData());
					if (writeBucket(bucketId, bucket)) {
						stash.remove_by_index(target.getIndex());
						logLshSingleEvict(target, leaf, bucketId);
						return true;
					}
					return false;
				}
				if (slot.getIndex() == -1) {
					slot.setLeaf_id(leaf);
					slot.setIndex(target.getIndex());
					slot.setData(target.getData());
					if (writeBucket(bucketId, bucket)) {
						stash.remove_by_index(target.getIndex());
						logLshSingleEvict(target, leaf, bucketId);
						return true;
					}
					return false;
				}
			}
		}
		return false;
	}

	/** Return bucket IDs from the global root down to the assigned leaf. */
	private List<Integer> pathFromRootToLeaf(int leaf) {
		List<Integer> path = new ArrayList<Integer>();
		for (int node = leaf; node >= 0; node = (node - 1) >> 1) {
			path.add(node);
			if (node == 0) break;
		}
		Collections.reverse(path);
		return path;
	}

	/** Build a leaf-to-root wire-format path, filling buckets in root-to-leaf priority. */
	private Bucket[] buildLshPathFromStash(int leaf, int excludedBlockIndex, boolean expiredOnly) {
		Bucket[] buckets = new Bucket[Configs.HEIGHT];
		int treeDepth = Configs.HEIGHT - 1;
		for (int pathIndex = 0; pathIndex < Configs.HEIGHT; pathIndex++) {
			ArrayList<Block> emptyBlocks = new ArrayList<Block>(Configs.Z);
			for (int slot = 0; slot < Configs.Z; slot++) emptyBlocks.add(new Block());
			buckets[pathIndex] = new Bucket(emptyBlocks);
		}

		for (int depth = 0; depth <= treeDepth; depth++) {
			int bucketId = leaf;
			for (int step = 0; step < treeDepth - depth; step++) {
				bucketId = (bucketId - 1) >> 1;
			}
			int pathIndex = treeDepth - depth; // wire format is leaf-to-root
			Block[] selected = expiredOnly
					? stash.remove_expired_by_bucket(bucketId, excludedBlockIndex)
					: stash.remove_by_bucket(bucketId, -1, excludedBlockIndex);
			ArrayList<Block> bucketBlocks = new ArrayList<Block>(Configs.Z);
			for (int slot = 0; slot < Configs.Z; slot++) bucketBlocks.add(selected[slot]);
			buckets[pathIndex] = new Bucket(bucketBlocks);
		}
		return buckets;
	}

	private Bucket readBucket(int bucketId) {
		byte[] idBytes = Ints.toByteArray(bucketId);
		byte[] header = MessageUtility.createMessageHeaderBytes(
				MessageUtility.ORAM_READBUCKET, idBytes.length);
		byte[] response = sendAndGetMessage(
				ByteBuffer.wrap(Bytes.concat(header, idBytes)), MessageUtility.ORAM_READBUCKET);
		if (response == null || response.length == 0) {
			return null;
		}
		return seria.bucketFromSerializable(response);
	}

	private boolean writeBucket(int bucketId, Bucket bucket) {
		byte[] idBytes = Ints.toByteArray(bucketId);
		byte[] bucketBytes = seria.bucketSerializable(bucket);
		byte[] body = Bytes.concat(idBytes, bucketBytes);
		byte[] header = MessageUtility.createMessageHeaderBytes(
				MessageUtility.ORAM_WRITEBUCKET, body.length);
		byte[] response = sendAndGetMessage(
				ByteBuffer.wrap(Bytes.concat(header, body)), MessageUtility.ORAM_WRITEBUCKET);
		return response != null && response.length > 0 && response[0] == (byte) 1;
	}

	private String hashForBlock(int blockIndex, byte[] data) {
		String hash = lshHashes[blockIndex];
		if (hash != null) {
			return hash;
		}
		ByteBuffer input = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
		float[] vector = new float[Configs.BLOCK_DATA_LEN / Float.BYTES];
		for (int i = 0; i < vector.length; i++) {
			vector[i] = input.getFloat();
		}
		hash = lsh.computeHash(vector);
		lshHashes[blockIndex] = hash;
		return hash;
	}

	private static final class HashClusterResult {
		final String[] medoids;
		final int[] assignment;

		HashClusterResult(String[] medoids, int[] assignment) {
			this.medoids = medoids;
			this.assignment = assignment;
		}
	}

	private static int hammingDistance(String left, String right) {
		if (left == null || right == null || left.length() != right.length()) {
			throw new IllegalArgumentException("hash length mismatch");
		}
		int distance = 0;
		for (int i = 0; i < left.length(); i++) {
			if (left.charAt(i) != right.charAt(i)) distance++;
		}
		return distance;
	}

	private HashClusterResult clusterHashes(String[] hashes, int clusterCount) {
		int hashCount = hashes.length;
		String[] medoids = new String[clusterCount];

		// Start at the observed hash with minimum total Hamming distance (the medoid).
		int firstMedoid = 0;
		int firstTotalDistance = Integer.MAX_VALUE;
		for (int candidate = 0; candidate < hashCount; candidate++) {
			int totalDistance = 0;
			for (int other = 0; other < hashCount; other++) {
				totalDistance += hammingDistance(hashes[candidate], hashes[other]);
			}
			if (totalDistance < firstTotalDistance) {
				firstTotalDistance = totalDistance;
				firstMedoid = candidate;
			}
		}
		medoids[0] = hashes[firstMedoid];

		// Initialize additional medoids farthest-first from the existing set.
		for (int c = 1; c < clusterCount; c++) {
			int farthest = 0;
			int farthestDistance = -1;
			for (int i = 0; i < hashCount; i++) {
				int nearestDistance = Integer.MAX_VALUE;
				for (int j = 0; j < c; j++) {
					nearestDistance = Math.min(nearestDistance,
							hammingDistance(hashes[i], medoids[j]));
				}
				if (nearestDistance > farthestDistance) {
					farthestDistance = nearestDistance;
					farthest = i;
				}
			}
			medoids[c] = hashes[farthest];
		}

		int[] assignment = new int[hashCount];
		Arrays.fill(assignment, -1);
		for (int iter = 0; iter < 20; iter++) {
			boolean changed = false;
			for (int i = 0; i < hashCount; i++) {
				int bestCluster = 0;
				int bestDistance = Integer.MAX_VALUE;
				for (int c = 0; c < clusterCount; c++) {
					int distance = hammingDistance(hashes[i], medoids[c]);
					if (distance < bestDistance) {
						bestDistance = distance;
						bestCluster = c;
					}
				}
				if (assignment[i] != bestCluster) {
					assignment[i] = bestCluster;
					changed = true;
				}
			}

			for (int c = 0; c < clusterCount; c++) {
				String bestMedoid = medoids[c];
				int bestTotalDistance = Integer.MAX_VALUE;
				for (int candidate = 0; candidate < hashCount; candidate++) {
					if (assignment[candidate] != c) continue;
					int totalDistance = 0;
					for (int other = 0; other < hashCount; other++) {
						if (assignment[other] == c) {
							totalDistance += hammingDistance(hashes[candidate], hashes[other]);
						}
					}
					if (totalDistance < bestTotalDistance) {
						bestTotalDistance = totalDistance;
						bestMedoid = hashes[candidate];
					}
				}
				medoids[c] = bestMedoid;
			}
			if (!changed) break;
		}
		return new HashClusterResult(medoids, assignment);
	}

	private double silhouetteScore(String[] hashes, HashClusterResult result) {
		double totalScore = 0.0;
		for (int i = 0; i < hashes.length; i++) {
			int ownCluster = result.assignment[i];
			int ownCount = 0;
			double ownDistance = 0.0;
			for (int j = 0; j < hashes.length; j++) {
				if (i != j && result.assignment[j] == ownCluster) {
					ownDistance += hammingDistance(hashes[i], hashes[j]);
					ownCount++;
				}
			}
			if (ownCount == 0) continue;
			double a = ownDistance / ownCount;
			double b = Double.POSITIVE_INFINITY;
			for (int c = 0; c < result.medoids.length; c++) {
				if (c == ownCluster) continue;
				int otherCount = 0;
				double otherDistance = 0.0;
				for (int j = 0; j < hashes.length; j++) {
					if (result.assignment[j] == c) {
						otherDistance += hammingDistance(hashes[i], hashes[j]);
						otherCount++;
					}
				}
				if (otherCount > 0) b = Math.min(b, otherDistance / otherCount);
			}
			if (Double.isFinite(b) && Math.max(a, b) > 0.0) {
				totalScore += (b - a) / Math.max(a, b);
			}
		}
		return totalScore / hashes.length;
	}

	/** Build Roost-compatible clusters from all data hashes before initial writes. */
	private void buildLshLeafClusters(byte[][] blockData) {
		if (!useLshLeafAssignment) return;
		List<String> validHashes = new ArrayList<String>();
		List<Integer> validBlockIds = new ArrayList<Integer>();
		for (int i = 0; i < blockData.length; i++) {
			if (blockData[i] == null || blockData[i].length < Configs.BLOCK_DATA_LEN) continue;
			validHashes.add(hashForBlock(i, blockData[i]));
			validBlockIds.add(i);
		}
		if (validHashes.size() < 3) {
			System.out.println("[LSH-AUTO-CLUSTER] insufficientHashes=" + validHashes.size()
					+ " fallback=bandMajority");
			return;
		}

		String[] hashes = validHashes.toArray(new String[0]);
		int maxClusters = Math.min(Configs.LEAF_COUNT, hashes.length - 1);
		HashClusterResult selected = null;
		double selectedScore = Double.NEGATIVE_INFINITY;
		System.out.println("[LSH-AUTO-CLUSTER-START] candidates=2-" + maxClusters
				+ " source=hashOnly metric=hamming selection=silhouette");
		for (int k = 2; k <= maxClusters; k++) {
			HashClusterResult candidate = clusterHashes(hashes, k);
			double score = silhouetteScore(hashes, candidate);
			System.out.println("[LSH-AUTO-CLUSTER] k=" + k
					+ " silhouette=" + String.format(java.util.Locale.ROOT, "%.4f", score));
			if (score > selectedScore) {
				selectedScore = score;
				selected = candidate;
			}
		}
		if (selected == null) return;

		lshLeafMedoids = selected.medoids;
		lshClusterCount = selected.medoids.length;
		lshClusterLeafStarts = new int[lshClusterCount];
		lshClusterLeafCounts = new int[lshClusterCount];
		int leafBits = 32 - Integer.numberOfLeadingZeros(Configs.LEAF_COUNT - 1);
		lshClusterPrefixBits = 32 - Integer.numberOfLeadingZeros(lshClusterCount - 1);
		int leavesPerCluster = 1 << (leafBits - lshClusterPrefixBits);
		for (int c = 0; c < lshClusterCount; c++) {
			// The cluster ID is the common K-bit root prefix. Any unused prefixes
			// (when k is not a power of two) leave their corresponding leaves unused.
			lshClusterLeafStarts[c] = c * leavesPerCluster;
			lshClusterLeafCounts[c] = leavesPerCluster;
		}
		System.out.println("[LSH-AUTO-CLUSTER-SELECTED] clusters=" + lshClusterCount
				+ " rootPrefixBits=" + lshClusterPrefixBits
				+ " leavesPerCluster=" + leavesPerCluster
				+ " usedLeaves=" + (lshClusterCount * leavesPerCluster)
				+ " silhouette=" + String.format(java.util.Locale.ROOT, "%.4f", selectedScore));
		for (int c = 0; c < lshClusterCount; c++) {
			StringBuilder blocks = new StringBuilder();
			for (int i = 0; i < hashes.length; i++) {
				if (selected.assignment[i] == c) {
					if (blocks.length() > 0) blocks.append(',');
					blocks.append(validBlockIds.get(i));
				}
			}
			System.out.println("[LSH-CLUSTER] cluster=" + c
					+ " rootPrefix=" + binaryPrefix(c, lshClusterPrefixBits)
					+ " leafRange=" + (Configs.LEAF_START + lshClusterLeafStarts[c])
					+ "-" + (Configs.LEAF_START + lshClusterLeafStarts[c]
						+ lshClusterLeafCounts[c] - 1)
					+ " medoid=" + lshLeafMedoids[c]
					+ " blocks=[" + blocks + "]");
		}
		System.out.println("[LSH-AUTO-CLUSTER-END]");
	}

	private int chooseLeafFromHash(String hash) {
		if (hash == null || hash.isEmpty()) {
			return rand.getRandomLeaf() + Configs.LEAF_START;
		}
		if (lshLeafMedoids == null || lshClusterLeafStarts == null
				|| lshClusterLeafCounts == null || lshLeafMedoids.length == 0) {
			int leafBits = 32 - Integer.numberOfLeadingZeros(Configs.LEAF_COUNT - 1);
			int leafLocal = 0;
			for (int bit = 0; bit < leafBits; bit++) {
				int ones = 0;
				int total = 0;
				for (int i = bit; i < hash.length(); i += leafBits) {
					if (hash.charAt(i) == '1') ones++;
					total++;
				}
				if (ones * 2 >= total) leafLocal |= 1 << bit;
			}
			return Configs.LEAF_START + (leafLocal % Configs.LEAF_COUNT);
		}
		int bestCluster = closestLshCluster(hash);
		int leafBits = 32 - Integer.numberOfLeadingZeros(Configs.LEAF_COUNT - 1);
		int suffixBits = leafBits - lshClusterPrefixBits;
		int suffix = 0;
		for (int i = 0; i < suffixBits; i++) {
			suffix = (suffix << 1) | (i < hash.length() && hash.charAt(i) == '1' ? 1 : 0);
		}
		int rangeCount = lshClusterLeafCounts[bestCluster];
		// Keep the hash-prefix choice as the base, but randomize the lower path bits
		// on every READ so a block does not remain on one deterministic leaf forever.
		int suffixMask = rangeCount - 1;
		int randomSuffix = rand.getRandomLeaf() & suffixMask;
		int leafOffset = Math.min(rangeCount - 1, suffix ^ randomSuffix);
		return Configs.LEAF_START + lshClusterLeafStarts[bestCluster] + leafOffset;
	}

	private static String binaryPrefix(int value, int bits) {
		String binary = Integer.toBinaryString(value);
		if (binary.length() >= bits) return binary;
		StringBuilder padded = new StringBuilder(bits);
		for (int i = binary.length(); i < bits; i++) padded.append('0');
		return padded.append(binary).toString();
	}

	private int closestLshCluster(String hash) {
		int bestCluster = 0;
		int bestDistance = Integer.MAX_VALUE;
		for (int i = 0; i < lshLeafMedoids.length; i++) {
			int distance = hammingDistance(hash, lshLeafMedoids[i]);
			if (distance < bestDistance) {
				bestDistance = distance;
				bestCluster = i;
			}
		}
		return bestCluster;
	}

	private void logLshRemap(int blockIndex, int accessedLeaf, int assignedLeaf, String hash) {
		if (!Boolean.getBoolean("pathoram.lsh.debug")) {
			return;
		}
		System.out.println("[LSH-REMAP] blockId=" + blockIndex
				+ " cluster=" + (lshLeafMedoids == null ? "band-majority"
						: closestLshCluster(hash))
				+ " rootPrefix=" + (lshLeafMedoids == null ? "fallback"
						: binaryPrefix(closestLshCluster(hash), lshClusterPrefixBits))
				+ " hashSuffixPrefix=" + hash.substring(0, Math.min(hash.length(),
						(32 - Integer.numberOfLeadingZeros(Configs.LEAF_COUNT - 1))
							- (lshLeafMedoids == null ? 0 : lshClusterPrefixBits)))
				+ " accessedLeaf=" + accessedLeaf
				+ " accessedPath=" + pathString(accessedLeaf)
				+ " assignedLeaf=" + assignedLeaf
				+ " assignedPath=" + pathString(assignedLeaf)
				+ " location=stash");
	}

	private void logLshSingleEvict(Block block, int leaf, int bucketId) {
		if (!Boolean.getBoolean("pathoram.lsh.debug")) {
			return;
		}
		System.out.println("[LSH-SINGLE-EVICT] blockId=" + block.getIndex()
				+ " assignedLeaf=" + leaf
				+ " assignedPath=" + pathString(leaf)
				+ " storedBucket=" + bucketId
				+ " location=server");
	}

	private String pathString(int leaf) {
		List<Integer> nodes = new ArrayList<Integer>();
		for (int node = leaf; node >= 0; node = (node - 1) >> 1) {
			nodes.add(node);
			if (node == 0) {
				break;
			}
		}
		Collections.reverse(nodes);
		return nodes.toString().replace(", ", "->").replace("[", "").replace("]", "");
	}

	private void logStashSnapshot(String stage, String context) {
		List<Block> blocks = stash.blocksSnapshot();
		blocks.sort((left, right) -> Integer.compare(left.getIndex(), right.getIndex()));
		StringBuilder snapshot = new StringBuilder();
		for (Block block : blocks) {
			if (snapshot.length() > 0) {
				snapshot.append(", ");
			}
			snapshot.append(block.getIndex());
		}
		System.out.println("[STASH-" + stage + "] " + context
				+ " size=" + blocks.size() + " blocks=[" + snapshot + "]");
	}

	private void writePathFromStash(int pathID) {
		writePathFromStash(pathID, -1);
	}

	private void writePathFromStash(int pathID, int preferredBlockIndex) {
		writePathFromStash(pathID, preferredBlockIndex, -1);
	}

	private void writePathFromStashForLeaf(int pathID, int preferredBlockIndex) {
		writePathFromStash(pathID, preferredBlockIndex, pathID);
	}

	private void writePathFromStash(int pathID, int preferredBlockIndex, int requiredLeaf) {
		Bucket[] bucketList = new Bucket[Configs.HEIGHT];
		int count = 0;
		for (int bucketId = pathID; bucketId >= 0; bucketId = (bucketId - 1) >> 1) {
			Block[] blocks = requiredLeaf < 0
					? stash.remove_by_bucket(bucketId, preferredBlockIndex)
					: stash.remove_by_bucket_for_leaf(bucketId, requiredLeaf, preferredBlockIndex);
			ArrayList<Block> bucketBlocks = new ArrayList<Block>(Configs.Z);
			for (int i = 0; i < Configs.Z; i++) {
				bucketBlocks.add(blocks[i]);
			}
			bucketList[count++] = new Bucket(bucketBlocks);
			if (bucketId == 0) {
				break;
			}
		}
		writePath(pathID, bucketList);
	}
	
	@Override
	public void readPath(int pathID) {
		// TODO Auto-generated method stub
		byte[] pathIdBytes = Ints.toByteArray(pathID);
		byte[] header = MessageUtility.createMessageHeaderBytes(MessageUtility.ORAM_READPATH,
				pathIdBytes.length);
		ByteBuffer requestBuffer = ByteBuffer.wrap(Bytes.concat(header, pathIdBytes));
		byte[] responseBytes = sendAndGetMessage(requestBuffer, MessageUtility.ORAM_READPATH);
		int startIndex = 0;
		int bucket_bytes_len = (4+4+Configs.BLOCK_DATA_LEN)*Configs.Z;
		for(int i=0;i<Configs.HEIGHT;i++){
			//recovery bucket from the response byte array
			byte[] bucket_bytes = Arrays.copyOfRange(
					responseBytes, startIndex, startIndex+bucket_bytes_len);
			startIndex += bucket_bytes_len;
			Bucket bucket = seria.bucketFromSerializable(bucket_bytes);
			ArrayList<Block> blocks = bucket.getBlocks();
			for(Block blk:blocks){
				//add real block to the stash
				if(blk.getIndex() != -1){
					stash.add(blk);
				}
			}
		}
		pathIdBytes = null;
		header = null;
		requestBuffer = null;
		responseBytes = null;
	}

	@Override
	public void writePath(int pathID, Bucket[] bucket_list) {
		// TODO Auto-generated method stub
		byte[] pathIdBytes = Ints.toByteArray(pathID);
		byte[][] pathBytes = new byte[Configs.HEIGHT][];
		for(int i=0;i<Configs.HEIGHT;i++){
			pathBytes[i] = seria.bucketSerializable(bucket_list[i]);
		}
		//join all bucket bytes
		byte[] pathBytesWrap = pathBytes[0];
		for(int i =1;i<Configs.HEIGHT;i++){
			pathBytesWrap = Bytes.concat(pathBytesWrap,pathBytes[i]);
		}
		pathBytesWrap = Bytes.concat(pathIdBytes,pathBytesWrap);
		byte[] header = MessageUtility.createMessageHeaderBytes(MessageUtility.ORAM_WRITEPATH,
				pathBytesWrap.length);
		ByteBuffer requestBuffer = ByteBuffer.wrap(Bytes.concat(header, pathBytesWrap));
		//send write path message
		byte[] responseBytes = sendAndGetMessage(requestBuffer, MessageUtility.ORAM_WRITEPATH);
		pathIdBytes = null;
		pathBytes = null;
		pathBytesWrap = null;
		header = null;
		requestBuffer = null;
		responseBytes = null;
	}
	
	@Override
	public void close() {
		try {
			if (mChannel != null && mChannel.isOpen()) {
				mChannel.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (mThreadGroup != null) {
			mThreadGroup.shutdown();
			try {
				if (!mThreadGroup.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
					mThreadGroup.shutdownNow();
				}
			} catch (InterruptedException e) {
				try {
					mThreadGroup.shutdownNow();
				} catch (IOException shutdownError) {
					e.addSuppressed(shutdownError);
				}
				Thread.currentThread().interrupt();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	public byte[] sendAndGetMessage(ByteBuffer requestBuffer, int messageType) {
		byte[] responseBytes = null;
		//send request to server
		try {
			while (requestBuffer.remaining() > 0) {
				Future requestBufferRead = mChannel.write(requestBuffer);
				try {
					requestBufferRead.get();
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// get response from server
			ByteBuffer typeAndSize = ByteBuffer.allocate(4 + 4);
			Future typeAndSizeRead = mChannel.read(typeAndSize);
			try {
				typeAndSizeRead.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			typeAndSize.flip();
			int[] typeAndSizeInt = MessageUtility.parseTypeAndLength(typeAndSize);
			int type = typeAndSizeInt[0];//message type
			int size = typeAndSizeInt[1];//data size
			typeAndSize = null;

			ByteBuffer responseBuffer = ByteBuffer.allocate(size);
			while (responseBuffer.remaining() > 0) {
				Future responseBufferRead = mChannel.read(responseBuffer);
				responseBufferRead.get();
			}
			responseBuffer.flip();
			//check if it is current request corresponding response
			if (type == messageType) {
				responseBytes = new byte[size];
				responseBuffer.get(responseBytes);//read data into byte array
			} else {
				System.out.println("client get wrong when resieve response from server!");
			}
			responseBuffer = null;
		} catch (Exception e) {
			try {
				mChannel.close();
			} catch (IOException e1) {
			}
		}
		return responseBytes;
	}
	
	public static void main(String[] args) {
		boolean useCache = true;
		boolean useLsh = false;
		String mode = "baseline-cache";
		if (args.length > 0) {
			switch (args[0].toLowerCase()) {
				case "cache":
				case "baseline-cache":
					useCache = true;
					mode = "baseline-cache";
					break;
				case "no-cache":
				case "baseline-no-cache":
					useCache = false;
					mode = "baseline-no-cache";
					break;
				case "lsh-cache":
					useCache = true;
					useLsh = true;
					mode = "lsh-cache";
					break;
				case "lsh-no-cache":
					useCache = false;
					useLsh = true;
					mode = "lsh-no-cache";
					break;
				default:
					System.out.println("Usage: Client [baseline-cache|baseline-no-cache|lsh-cache|lsh-no-cache] [accessSeed] [transitionProbability]");
					return;
			}
		}

		long accessSeed;
		try {
			accessSeed = args.length > 1 ? Long.parseLong(args[1]) : new SecureRandom().nextLong();
		} catch (NumberFormatException e) {
			System.out.println("[ERROR] accessSeed must be a signed 64-bit integer: " + args[1]);
			return;
		}

		double transitionProbability = 0.20;
		if (args.length > 2) {
			try {
				transitionProbability = Double.parseDouble(args[2]);
			} catch (NumberFormatException e) {
				System.out.println("[ERROR] transitionProbability must be between 0.0 and 1.0: " + args[2]);
				return;
			}
			if (!Double.isFinite(transitionProbability)
					|| transitionProbability < 0.0 || transitionProbability > 1.0) {
				System.out.println("[ERROR] transitionProbability must be between 0.0 and 1.0: " + args[2]);
				return;
			}
		}
		if (args.length > 3) {
			System.out.println("[ERROR] LSH cluster count is selected automatically; no fourth argument is accepted.");
			return;
		}

		Path dataDirectory = Paths.get("data");
		if (!Files.isDirectory(dataDirectory)) {
			dataDirectory = Paths.get("..", "data");
		}
		byte[][] blockData = new byte[Configs.BLOCK_COUNT][];
		try {
			for (int i = 0; i < Configs.BLOCK_COUNT; i++) {
				blockData[i] = loadBinAsBlockData(dataDirectory.resolve(i + ".bin"));
			}
		} catch (IOException e) {
			System.err.println("[ERROR] Could not load all 48 data blocks from " + dataDirectory);
			e.printStackTrace();
			return;
		}

		long oramSeed = accessSeed ^ 0x9E3779B97F4A7C15L;
		Client client = new Client(useCache, useLsh, oramSeed);
		client.initServer();
		client.buildLshLeafClusters(blockData);
		for (int i = 0; i < Configs.BLOCK_COUNT; i++) {
			client.obliviousAccess(i, blockData[i], Operation.WRITE);
		}

		int[][] groups = createContiguousAccessGroups(Configs.BLOCK_COUNT, 3);
		Random accessRandom = new Random(accessSeed);
		int currentGroup = 0;
		int transitions = 0;
		int dataMismatches = 0;
		int readIterations = 300;
		for (int i = 0; i < readIterations; i++) {
			if (accessRandom.nextDouble() < transitionProbability) {
				int previousGroup = currentGroup;
				do {
					currentGroup = accessRandom.nextInt(groups.length);
				} while (currentGroup == previousGroup);
				transitions++;
			}
			int[] group = groups[currentGroup];
			int blockIndex = group[accessRandom.nextInt(group.length)];
			byte[] actual = client.obliviousAccess(
					blockIndex, new byte[Configs.BLOCK_DATA_LEN], Operation.READ);
			if (!Arrays.equals(actual, blockData[blockIndex])) {
				dataMismatches++;
			}
		}

		System.out.println("[EXPERIMENT] mode=" + mode
				+ " lshClusters=" + client.getLshClusterCount()
				+ " lshAssignments=" + client.getLshAssignmentCount()
				+ " reads=" + readIterations
				+ " transitionProbability=" + transitionProbability
				+ " transitions=" + transitions
				+ " accessSeed=" + accessSeed
				+ " oramSeed=" + oramSeed);
		System.out.println("[SUMMARY] cache=" + useCache
				+ " lsh=" + useLsh
				+ " cacheHits=" + client.getCacheHitCount()
				+ " cacheHitRate=" + client.getCacheHitRate()
				+ " serverReads=" + client.getServerReadCount()
				+ " singleEvictAttempts=" + client.getSingleEvictAttempts()
				+ " singleEvictSuccesses=" + client.getSingleEvictSuccesses()
				+ " fallbackPathEvicts=" + client.getFallbackPathEvictCount()
				+ " stashSize=" + client.stash.size()
				+ " dataMismatches=" + dataMismatches);
		client.close();
	}

	private static byte[] loadBinAsBlockData(Path path) throws IOException {
		byte[] input = Files.readAllBytes(path);
		byte[] block = new byte[Configs.BLOCK_DATA_LEN];
		System.arraycopy(input, 0, block, 0, Math.min(input.length, block.length));
		return block;
	}

	private static int[][] createContiguousAccessGroups(int blockCount, int groupCount) {
		if (blockCount <= 0 || groupCount <= 0 || blockCount % groupCount != 0) {
			throw new IllegalArgumentException("blockCount must be divisible by groupCount");
		}
		int blocksPerGroup = blockCount / groupCount;
		int[][] groups = new int[groupCount][blocksPerGroup];
		for (int group = 0; group < groupCount; group++) {
			for (int offset = 0; offset < blocksPerGroup; offset++) {
				groups[group][offset] = group * blocksPerGroup + offset;
			}
		}
		return groups;
	}

}
