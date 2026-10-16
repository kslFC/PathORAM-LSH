package com.pathoram;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * stash is a temporary storage in the client
 * client stores path into the stash
 * client write path back to the server from the stash
 */
public class Stash {

	private Map<Integer,Block> stash_hash;
	
	public Stash(){
		stash_hash  = new HashMap<Integer,Block>();
	}
	
	public void add(Block block) {
		//stash block is always the freshest block
		if (!stash_hash.containsKey(block.getIndex())) {
			block.setEvictCount(Configs.DEFAULT_DELAYED_EVICT_COUNT);
			stash_hash.put(block.getIndex(), block);
		}
	}
	
	public Block find_by_index(int index) {
		if (!stash_hash.containsKey(index)) {
			return null;
		}
		return stash_hash.get(index);
	}

	/** Number of blocks currently available for cache hits or later path eviction. */
	public int size() {
		return stash_hash.size();
	}

	/** Snapshot used while selecting delayed-eviction candidates. */
	public List<Block> blocksSnapshot() {
		return new ArrayList<Block>(stash_hash.values());
	}

	/** Remove and return one block, for future single-block eviction support. */
	public Block remove_by_index(int index) {
		return stash_hash.remove(index);
	}
	
	public void updataBlock(Block block) {
		if (stash_hash.containsKey(block.getIndex())) {
			stash_hash.remove(block.getIndex());
		}
		stash_hash.put(block.getIndex(), block);
	}
	
	public Block[] remove_by_bucket(int position) {
		return remove_by_bucket(position, -1, -1);
	}

	/** Remove eligible blocks, preferring the expired block that triggered an eviction. */
	public Block[] remove_by_bucket(int position, int preferredIndex) {
		return remove_by_bucket(position, preferredIndex, -1);
	}

	/** Remove eligible blocks except one deliberately retained in the READ cache. */
	public Block[] remove_by_bucket(int position, int preferredIndex, int excludedIndex) {
		return remove_by_bucket(position, preferredIndex, excludedIndex, -1, false);
	}

	/** Remove blocks assigned to one exact leaf path, used by grouped single eviction. */
	public Block[] remove_by_bucket_for_leaf(int position, int leaf, int preferredIndex) {
		return remove_by_bucket(position, preferredIndex, -1, leaf, false);
	}

	/** Normal READ writeback may drain only blocks whose delayed-evict count expired. */
	public Block[] remove_expired_by_bucket(int position, int excludedIndex) {
		return remove_by_bucket(position, -1, excludedIndex, -1, true);
	}

	private Block[] remove_by_bucket(int position, int preferredIndex, int excludedIndex,
			int requiredLeaf, boolean expiredOnly) {
		Block[] stash_list = new Block[Configs.Z];
		for (int i = 0; i < Configs.Z; i++) {
			stash_list[i] = new Block();
		}
		int count = 0;
		Block preferred = stash_hash.get(preferredIndex);
		if (preferredIndex != excludedIndex && preferred != null
				&& isEligibleForBucket(preferred, position, requiredLeaf, expiredOnly)) {
			stash_list[count++] = preferred;
		}
		for (Integer index : stash_hash.keySet()) {
			if (index == preferredIndex || index == excludedIndex) {
				continue;
			}
			if (isEligibleForBucket(stash_hash.get(index), position, requiredLeaf, expiredOnly)) {
				stash_list[count] = stash_hash.get(index);
				count++;
			}
			if (count == Configs.Z)//bucket is full
				break;
		}
		// remove blocks from stash
		for (int i = 0; i < Configs.Z; i++) {
			if (stash_list[i].getIndex() != -1) {
				stash_hash.remove(stash_list[i].getIndex());
			}
		}
		return stash_list;
	}

	private boolean isEligibleForBucket(Block block, int bucket, int requiredLeaf, boolean expiredOnly) {
		int leaf = block.getLeaf_id();
		return (!expiredOnly || block.getEvictCount() <= 0)
				&& (requiredLeaf < 0 || leaf == requiredLeaf) && isOnPath(leaf, bucket);
	}

	private boolean isOnPath(int leaf, int bucket) {
		for (int pos = leaf; pos >= 0; pos = (pos - 1) >> 1) {
			if (pos == bucket) {
				return true;
			}
			if (pos == 0) {
				break;
			}
		}
		return false;
	}
}
