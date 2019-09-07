/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.blockstorage.memorymapped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.blockstorage.MemoryMappedStorage;

/**
 * The class that handles the management of the list of free nodes.
 * 
 */
public class MMFreeList extends FreeNode {

	/* package private for testing */
	final List<FreeNode> pages;
	private final BufferFactory bufferFactory;
	private final static Logger LOG = LoggerFactory.getLogger(MMFreeList.class);

	/**
	 * Constructor
	 * 
	 * @param factory the buffer factory this list should use.
	 * @throws IOException on error
	 */
	public MMFreeList(BufferFactory factory) throws IOException {
		super(factory.readBuffer(new MMPosition(0)));
		LOG.debug("Creating FreeList");
		this.pages = new ArrayList<FreeNode>();

		this.bufferFactory = factory;
		long nextBlock = this.nextBlock();
		if (nextBlock > 0) {
			LOG.debug("Reading next FreeNode {}", nextBlock);
			FreeNode node = new FreeNode(factory.readBuffer(new MMPosition(nextBlock)));
			pages.add(node);
			nextBlock = node.nextBlock();
		}
		verify();
		LOG.debug("FreeList created");
	}

	/**
	 * Verify that the list contains what appear to be valid record numbers.
	 */
	private void verify() {

		Set<Long> seen = new TreeSet<Long>();
		List<Long> recs = WrappedIterator.create(getRecords()).toList();
		if (recs.size() != count()) {
			throw new IllegalStateException("Root count() does not equal records count");
		}

		seen.addAll(recs);
		if (seen.size() != recs.size()) {
			throw new IllegalStateException("possible duplicate records in free list");
		}

		for (int i = 0; i < pages.size(); i++) {
			FreeNode node = pages.get(i);
			recs = WrappedIterator.create(node.getRecords()).toList();
			if (recs.size() != node.count()) {
				throw new IllegalStateException("Page " + i + " count() does not equal records count");

			}
			int oldCount = seen.size();
			seen.addAll(recs);
			int newCount = seen.size() - oldCount;
			if (newCount != recs.size()) {
				throw new IllegalStateException("possible duplicate records on page " + i + " in free list");
			}

		}
		for (Long l : seen) {
			if ((l % MemoryMappedStorage.BLOCK_SIZE) != 0) {
				throw new IllegalStateException("Invalid block number " + l);
			}
		}
		LOG.info("FreeList verified {} free nodes", seen.size());
	}

	/**
	 * Get the number of blocks in the free list.
	 * 
	 * @return the number of blocks in the free list.
	 */
	public int blockCount() {
		return count() + pages.stream().mapToInt(FreeNode::count).sum();
	}

	/**
	 * Get an iterator of the free blocks.
	 * 
	 * @return an iterator of the free blocks.
	 */
	public Iterator<Long> getFreeBlocks() {
		if (pages.isEmpty()) {
			return this.getRecords();
		}
		return WrappedIterator.create(this.getRecords())
				.andThen(WrappedIterator.createIteratorIterator(pages.stream().map(FreeNode::getRecords).iterator()));

	}

	/**
	 * The number of bytes represented by all the blocks on the free list.
	 * 
	 * @return the number of free bytes available.
	 */
	public long freeSpace() {
		return blockCount() * (long) MemoryMappedStorage.BLOCK_SIZE;
	}

	/**
	 * Get the next free block from the list.
	 * 
	 * @return the offset of the next block or null if no blocks are available.
	 */
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
			// if the free node is empty use it as the next block
			if (freeNode.isEmpty()) {
				pages.remove(idx);
				idx--;
				// clear the next block pointer
				if (idx < 0) {
					// only root page remains
					this.nextBlock(0);
				} else {
					FreeNode prev = pages.get(idx);
					prev.nextBlock(0);
				}
				return freeNode.offset();
			}
		}
		LongBuffer lb = freeNode.getFreeRecords();
		int pos = freeNode.count() - 1;
		long retval = lb.get(pos);
		freeNode.count(pos);
		lb.put(pos, 0L);
		freeNode.buffUsed(freeNode.buffUsed() - Long.BYTES);
		return retval;
	}

	/**
	 * Add the offset to the list of free nodes.
	 * 
	 * @param offset the offset to add.
	 * @throws IOException on error
	 */
	public void add(Long offset) throws IOException {
		FreeNode freeNode = null;
		if (pages.isEmpty()) {
			freeNode = this;
		} else {
			freeNode = pages.get(pages.size() - 1);
		}
		LongBuffer lb = freeNode.getFreeRecords();
		int max = lb.capacity();
		if (max == freeNode.count()) {
			FreeNode node = new FreeNode(bufferFactory.createBuffer());
			node.clearData();
			freeNode.nextBlock(node.offset());
			pages.add(node);
			node.count(0);
			node.buffUsed(0);
			freeNode = node;
			lb = freeNode.getFreeRecords();
		}
		lb.put(freeNode.count(), offset);
		freeNode.count(freeNode.count() + 1);
		freeNode.buffUsed(freeNode.buffUsed() + Long.BYTES);
	}

	@Override
	public String toString() {
		return String.format("FL[ o:%s u:%s l:%s c:%s fc:%s]", offset(), buffUsed(), getBuffer().limit(),
				getBuffer().capacity(), count());
	}

	/**
	 * the methods the FreeList buffer factory must provide.
	 *
	 */
	public interface BufferFactory {
		/**
		 * Create a new buffer.
		 * 
		 * @return a new empty buffer.
		 * @throws IOException on error
		 */
		ByteBuffer createBuffer() throws IOException;

		/**
		 * Read the specified buffer.
		 * 
		 * @param offset the buffer record/offset to read.
		 * @return the buffer
		 * @throws IOException on error
		 */
		ByteBuffer readBuffer(MMPosition offset) throws IOException;
	}
}
