package org.xenei.blockstorage.memorymapped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

import org.xenei.blockstorage.MemoryMappedStorage;

public class MMFreeList implements FreeNode {

	private final List<InnerNode> pages;
	private final BlockHeader root;
	private final FileChannel fileChannel;

	private static final int NEXT_BLOCK_OFFSET = BlockHeader.HEADER_SIZE;
	private static final int COUNT_OFFSET = NEXT_BLOCK_OFFSET + Long.BYTES;;
	private static final int DATA_OFFSET = COUNT_OFFSET + Integer.BYTES;
	private static final int DATA_SIZE = MemoryMappedStorage.BLOCK_SIZE - DATA_OFFSET;

	public MMFreeList(FileChannel fileChannel, BlockHeader root) throws IOException {
		this.pages = new ArrayList<InnerNode>();
		this.root = root;
		this.fileChannel = fileChannel;
		long nextBlock = this.nextBlock();
		if (nextBlock != 0) {
			InnerNode node = new InnerNode(
					fileChannel.map(MapMode.READ_WRITE, nextBlock, MemoryMappedStorage.BLOCK_SIZE));
			pages.add(node);
			nextBlock = node.nextBlock();
		}
	}

	@Override
	public BlockHeader header() {
		return root;
	}

	public int blockCount() {
		return localCount() + pages.stream().mapToInt(InnerNode::localCount).sum();
	}

	@Override
	public int localCount() {
		return root.getBuffer().getInt(COUNT_OFFSET);
	}

	@Override
	public void localCount(int count) {
		root.getBuffer().putInt(COUNT_OFFSET, count);
	}

	@Override
	public boolean isEmpty() {
		return root.getBuffer().getInt(COUNT_OFFSET) > 0;
	}

	public long freeSpace() {
		return blockCount() * (long) DATA_SIZE;
	}

	private long nextBlock() {
		return root.getBuffer().getLong(NEXT_BLOCK_OFFSET);
	}

	@Override
	public void nextBlock(long offset) {
		root.getBuffer().putLong(NEXT_BLOCK_OFFSET, offset);
	}

	@Override
	public LongBuffer getData() {
		return root.getBuffer().position(DATA_OFFSET).asLongBuffer();
	}

	public Long getBlock() {
		FreeNode freeNode = null;
		if (pages.isEmpty()) {
			if (this.isEmpty()) {
				return null; // no free blocks
			}
			freeNode = this;
		} else {
			int idx = pages.size() - 1;
			freeNode = pages.get(idx);
			if (freeNode.isEmpty()) {
				pages.remove(idx);
				idx--;
				if (idx < 0) {
					// only page is empty
					this.nextBlock(0);
				} else {
					FreeNode prev = pages.get(idx);
					prev.nextBlock(0);
				}
				return freeNode.header().offset();
			}
		}
		LongBuffer lb = freeNode.getData();
		int pos = freeNode.localCount();
		long retval = lb.get(pos);
		lb.put(pos, 0L);
		freeNode.localCount(pos - 1);
		freeNode.header().buffUsed(freeNode.header().buffUsed() - Long.BYTES);
		return retval;
	}

	public void add(Long offset) throws IOException {
		FreeNode freeNode = null;
		if (pages.isEmpty()) {
			freeNode = this;
		} else {
			freeNode = pages.get(pages.size() - 1);
		}
		LongBuffer lb = freeNode.getData();
		int max = lb.capacity();
		if (max == freeNode.localCount()) {
			InnerNode node = new InnerNode(
					fileChannel.map(MapMode.READ_WRITE, fileChannel.size(), MemoryMappedStorage.BLOCK_SIZE));
			node.header().clearData();
			freeNode.nextBlock(node.header().offset());
			pages.add(node);
			node.localCount(0);
			node.header.buffUsed(0);
			freeNode = node;
		}
		freeNode.localCount(freeNode.localCount() + 1);
		freeNode.getData().put(freeNode.localCount(), offset);
		freeNode.header().buffUsed(freeNode.header().buffUsed() + Long.BYTES);
	}

	private class InnerNode implements FreeNode {
		private BlockHeader header;

		InnerNode(ByteBuffer buffer) throws IOException {
			this.header = new BlockHeader(buffer);
		}

		@Override
		public BlockHeader header() {
			return header;
		}

		@Override
		public boolean isEmpty() {
			return header.getBuffer().getInt(COUNT_OFFSET) > 0;
		}

		@Override
		public int localCount() {
			return header.getBuffer().getInt(COUNT_OFFSET);
		}

		@Override
		public void localCount(int count) {
			header.getBuffer().putInt(COUNT_OFFSET, count);
		}

		private long nextBlock() {
			return header.getBuffer().getLong(NEXT_BLOCK_OFFSET);
		}

		@Override
		public void nextBlock(long offset) {
			header.getBuffer().putLong(NEXT_BLOCK_OFFSET, offset);
		}

		@Override
		public LongBuffer getData() {
			return header.getBuffer().position(DATA_OFFSET).asLongBuffer();
		}
	}
}
