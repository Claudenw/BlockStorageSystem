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
import java.util.Arrays;
import org.xenei.blockstorage.MemoryMappedStorage;

/**
 * A block header is the first set of data in a block and is used by the
 * system to track it.
 *
 * The header contains the offset for the block, the number of bytes used
 * in the block.
 * 
 * The block is used both in the FreeList and in writing data.
 * 
 */
public class BlockHeader {
	private static byte[] CLEAN_BUFFER;
	// integer at end of header size is accounted for in FreeNode 
	public static final int HEADER_SIZE = 2 * Long.BYTES + Integer.SIZE;
	public static final int BLOCK_SPACE = MemoryMappedStorage.BLOCK_SIZE - HEADER_SIZE;
	private static final int OFFSET_OFFSET = 0;
	private static final int USED_OFFSET = OFFSET_OFFSET + Long.BYTES;

	private final ByteBuffer buffer;

	static {
		CLEAN_BUFFER = new byte[MemoryMappedStorage.BLOCK_SIZE];
		Arrays.fill(CLEAN_BUFFER, (byte) 0);
	}

	/**
	 * Create a block header from the buffer and the specified offset. The offset
	 * should be the position from which the buffer was read.
	 * 
	 * @param offset the offset of the buffer in the file.
	 * @param buffer the buffer.
	 * @throws IOException
	 */
	public BlockHeader(ByteBuffer buffer) throws IOException {
		this.buffer = buffer;
	}

	public void offset(long offset) {
		buffer.putLong(OFFSET_OFFSET, offset);
	}

	public long offset() {
		return buffer.getLong(OFFSET_OFFSET);
	}

	/**
	 * Set the number of bytes used in the buffer. This may be less than the size of
	 * the buffer.
	 * 
	 * @param buffUsed the number of bytes used.
	 */
	public void buffUsed(int buffUsed) {
		buffer.putInt(USED_OFFSET, buffUsed);
	}

	/**
	 * Get the number of bytes used in the buffer. This may be less than the size of
	 * the buffer.
	 * 
	 * @return the number of bytes used.
	 */
	public int buffUsed() {
		return buffer.getInt(USED_OFFSET);
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}
	
	@Override
	public String toString() {
		return String.format( "H[ o:%s u:%s l:%s c:%s]", offset(), buffUsed(), buffer.limit(), buffer.capacity());
	}

	/**
	 * Clear (fill with null) the entire buffer from the beginning.
	 */
	public void clear() {
		buffer.position(0);
		doClear();
	}
	
	/**
	 * Clear (fill with null) the buffer from the current position.
	 */
	private void doClear() {
		while (buffer.hasRemaining())
		{
			int limit = Integer.min(buffer.remaining(), MemoryMappedStorage.BLOCK_SIZE);
			buffer.put(CLEAN_BUFFER, 0, limit);
		}
	}

	/**
	 * Clear (fill with null) all user data in buffer.
	 * This is all the data after the header. 
	 */
	public void clearData() {
		buffer.position(HEADER_SIZE);
		doClear();
	}
}
