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
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

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
	 * Constructor.
	 * @param fileChannel The file channel to use.
	 * @throws IOException on error.
	 */
	public MMFreeList(FileChannel fileChannel) throws IOException {
		this(new FileListBufferFactory( fileChannel));
	}
	
	/** TESTING ONLY 
	 * @throws IOException on error. 
	 **/
	MMFreeList( BufferFactory factory) throws IOException {
		super( factory.readBuffer(0) );
		this.pages = new ArrayList<FreeNode>();

		this.bufferFactory = factory;
		long nextBlock = this.nextBlock();
		if (nextBlock > 0) {
			FreeNode node = new FreeNode( factory.readBuffer( nextBlock ) );
			pages.add(node);
			nextBlock = node.nextBlock();
		}
		
	}

	/**
	 * Get the number of blocks in the free list.
	 * @return the number of blocks in the free list.
	 */
	public int blockCount() {
		return count() + pages.stream().mapToInt(FreeNode::count).sum();
	}

	/**
	 * The number of bytes represented by all the blocks on the free list.
	 * @return the number of free bytes available.
	 */
	public long freeSpace() {
		return blockCount() * (long) MemoryMappedStorage.BLOCK_SIZE;
	}

	/**
	 * Get the next free block from the list.
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
				return freeNode.offset();
			}
		}
		LongBuffer lb = freeNode.getFreeRecords();
		int pos = freeNode.count()-1;
		long retval = lb.get(pos);
		freeNode.count(pos);
		lb.put(pos, 0L);
		freeNode.buffUsed(freeNode.buffUsed() - Long.BYTES);
		return retval;
	}

	/**
	 * Add the offset to the list of free nodes.
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
			FreeNode node = new FreeNode( bufferFactory.createBuffer() );
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
		return String.format( "FL[ o:%s u:%s l:%s c:%s fc:%s]", offset(), buffUsed(), getBuffer().limit(), getBuffer().capacity(), count());
	}

	/**
	 * The buffer factory for the free list.
	 *
	 */
	public interface BufferFactory {
		/**
		 * Create a buffer.
		 * @return the new byte buffer.
		 * @throws IOException on error.
		 */
		ByteBuffer createBuffer() throws IOException;
		
		/**
		 * Read a specific buffer.
		 * @param offset the buffer to read.
		 * @return the read buffer.
		 * @throws IOException on error.
		 */
		ByteBuffer readBuffer(long offset) throws IOException;
	}
	
	/**
	 * The BufferFactory for the MMFreeList.
	 *
	 */
	private static class FileListBufferFactory implements BufferFactory {
		
		private final FileChannel fileChannel;

		/**
		 * Constructor.
		 * @param fileChannel the file channel to use.
		 */
		FileListBufferFactory(FileChannel fileChannel)
		{
			this.fileChannel = fileChannel;
			
		}

		@Override
		public ByteBuffer createBuffer() throws IOException {
			LOG.debug( "Creating buffer at {}", fileChannel.size());
			return fileChannel.map(MapMode.READ_WRITE, fileChannel.size(), MemoryMappedStorage.BLOCK_SIZE);
		}

		@Override
		public ByteBuffer readBuffer(long offset) throws IOException {
			LOG.debug( "Reading buffer at {}", offset );
			return fileChannel.map(MapMode.READ_WRITE, offset, MemoryMappedStorage.BLOCK_SIZE);
		}
		
	}
}
