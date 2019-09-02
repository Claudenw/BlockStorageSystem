package org.xenei.blockstorage.memorymapped;

import java.nio.LongBuffer;

public interface FreeNode {
	LongBuffer getData();

	BlockHeader header();

	boolean isEmpty();

	void nextBlock(long offset);

	void localCount(int count);

	int localCount();
}