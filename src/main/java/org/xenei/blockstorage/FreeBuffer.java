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
package org.xenei.blockstorage;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Iterator;
import java.util.TreeSet;

import org.xenei.span.LongSpan;
import org.xenei.span.Span;
import org.xenei.spanbuffer.AbstractSpanBuffer;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * An Span buffer that wraps the set of free blocks so that it is easy to
 * read/write from the storage layer.  Free blocks are blocks that were allocated
 * but have since been freed.
 *
 */
class FreeBuffer extends AbstractSpanBuffer {

	private TreeSet<LongSpan> freeBlocks;

	/**
	 * Constructor.
	 */
	public FreeBuffer() {
		super(0);
		freeBlocks = new TreeSet<LongSpan>(LongSpan.COMPARATOR_BY_OFFSET);
	}

	/**
	 * Constructor.
	 * @param data the spanbuffer that contains the free buffer data.
	 * @throws IOException
	 */
	public FreeBuffer(SpanBuffer data) throws IOException {
		super(0);
		freeBlocks = new TreeSet<LongSpan>(LongSpan.COMPARATOR_BY_OFFSET);
		DataInputStream dis = new DataInputStream(data.getInputStream());
		byte[] buffer = new byte[LongSpan.BYTES * 100];
		ByteBuffer buff = ByteBuffer.wrap(buffer);
		LongBuffer lBuff = buff.asLongBuffer();
		while (dis.available() > 0) {
			int read = dis.read(buffer);
			if (read == -1) {
				break;
			}
			if (read > 0) {
				lBuff.position(0);
				int limit = read / LongSpan.BYTES;
				for (int i = 0; i < limit; i++) {
					long offset = lBuff.get();
					if (offset == 0) {
						break;
					}
					long length = lBuff.get();
					freeBlocks.add(LongSpan.fromLength(offset, length));
				}
			}
		}
	}

	private FreeBuffer(long offset, TreeSet<LongSpan> freeBlocks) {
		super(offset);
		this.freeBlocks = freeBlocks;
	}

	/**
	 * Get the number of freed blocks 
	 * @return the number of freed blocks.
	 */
	public int getBlockCount() {
		return freeBlocks.size();
	}

	/**
	 * Get the next free block.
	 * @return the Span for the next free block
	 */
	public LongSpan getBlock() {
		return freeBlocks.pollFirst();
	}

	/**
	 * Add a span to the list of free blocks.
	 * @param block the block to add.
	 */
	public void add(LongSpan block) {
		freeBlocks.add(block);
	}

	/**
	 * See if there are no more free blocks.
	 * @return true if there are no free blocks available.
	 */
	public boolean isEmpty() {
		return freeBlocks.isEmpty();
	}

	@Override
	public SpanBuffer duplicate(long newOffset) {
		if (newOffset == getOffset()) {
			return this;
		}
		return new FreeBuffer(newOffset, freeBlocks);
	}

	@Override
	public SpanBuffer sliceAt(final long position) {
		if (position == getOffset()) {
			return this;
		}
		if (position == (getOffset() + getLength())) {
			return Factory.EMPTY.duplicate(getOffset() + getLength());
		}
		return new FreeBuffer(getLength() - position, freeBlocks);

	}

	@Override
	public SpanBuffer head(final long byteCount) {
		if ((byteCount < 0) || (byteCount > getLength())) {
			throw new IllegalArgumentException(
					String.format("byte count %s is not in the range [0,%s]", byteCount, getLength()));
		}
		return new FreeBuffer(byteCount, freeBlocks);
	}

	private LongSpan getReadSpan(long position) {
		LongSpan span = freeBlocks.first();
		int blockNumber = (int) position / LongSpan.BYTES;
		if (blockNumber > 0) {
			if (blockNumber > freeBlocks.size()) {
				throw new IllegalStateException(
						String.format("block %s beyond end %s", blockNumber, freeBlocks.size()));
			} else if (blockNumber == freeBlocks.size()) {
				span = freeBlocks.last();
			} else {
				span = (LongSpan) freeBlocks.toArray()[blockNumber];
			}
		}
		return span;
	}

	@Override
	public byte read(long position) throws IOException {
		if (position > getEnd()) {
			throw new IOException(
					String.format("Position %s is past the end of the buffer (%s)", position, getEnd()));
		}

		int byteOffset = (int) position % LongSpan.BYTES;
		return Span.Util.asByteBuffer(getReadSpan(position)).get(byteOffset);
	}

	@Override
	public int read(long position, byte[] buff, int pos, int len) {
		if (position > getEnd()) {
			return 0;
		}
		int insertPosition = pos;
		int insertLen = len;
		int read = 0;
		LongSpan span = getReadSpan(position);
		Iterator<LongSpan> rest = null;
		int byteOffset = (int) position % LongSpan.BYTES;
		if (byteOffset != 0) {
			int limit = LongSpan.BYTES - byteOffset;
			if (limit < len) {
				limit = len;
			}
			System.arraycopy(Span.Util.asByteBuffer(span).array(), byteOffset, buff, insertPosition, limit);
			insertPosition += limit;
			insertLen -= limit;
			read = limit;
			rest = freeBlocks.tailSet(span, false).iterator();
		} else {
			rest = freeBlocks.tailSet(span, true).iterator();
		}

		while (insertLen > 0 && rest.hasNext()) {
			span = rest.next();
			if (insertLen >= LongSpan.BYTES) {
				System.arraycopy(Span.Util.asByteBuffer(span).array(), 0, buff, insertPosition, LongSpan.BYTES);
				insertPosition += LongSpan.BYTES;
				insertLen -= LongSpan.BYTES;
				read += LongSpan.BYTES;
			} else {
				System.arraycopy(Span.Util.asByteBuffer(span).array(), 0, buff, insertPosition, insertLen);
				read += insertLen;
				insertLen = 0;
			}
		}
		return read;
	}

	@Override
	public long getLength() {
		return (freeBlocks.size() * 2 * Long.BYTES) - getOffset();
	}

	@Override
	public long getEnd() {
		return LongSpan.calcEnd(this);
	}

	/**
	 * Get the number of bytes in all of the free blocks combined. 
	 * @return the number of free bytes.
	 */
	public long getFreeSpace() {
		return freeBlocks.stream().mapToLong(LongSpan::getLength).sum();
	}

}