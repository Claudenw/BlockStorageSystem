package org.xenei.blockstorage;

public interface Stats {

	long dataLength();

	long deletedBlocks();

	long freeSpace();

}