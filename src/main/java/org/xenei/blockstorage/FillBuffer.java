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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.AbstractSpanBuffer;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * A virtual span buffer that returns a specified number of null bytes (0)
 *
 */
class FillBuffer extends AbstractSpanBuffer {

	long length;

	/**
	 * Constructor
	 * 
	 * @param length the number of bytes in the buffer.
	 */
	public FillBuffer(long length) {
		super(0);
		this.length = length;
	}

	/**
	 * Constructor
	 * 
	 * @param length the number of bytes in the buffer.
	 * @param offset the offset for the buffer.
	 */
	private FillBuffer(long length, long offset) {
		super(offset);
		this.length = length;
	}

	@Override
	public SpanBuffer duplicate(long newOffset) {
		if (newOffset == getOffset()) {
			return this;
		}
		return new FillBuffer(newOffset, this.length);
	}

	@Override
	public SpanBuffer sliceAt(final long position) {
		if (position == getOffset()) {
			return this;
		}
		if (position == (getOffset() + getLength())) {
			return Factory.EMPTY.duplicate(getOffset() + getLength());
		}
		return new FillBuffer(getLength() - position, position);

	}

	@Override
	public SpanBuffer head(final long byteCount) {
		if ((byteCount < 0) || (byteCount > getLength())) {
			throw new IllegalArgumentException(
					String.format("byte count %s is not in the range [0,%s]", byteCount, getLength()));
		}
		return new FillBuffer(byteCount, getOffset());
	}

	@Override
	public byte read(long position) throws IOException {
		if (position > getEnd()) {
			throw new IOException(String.format("Position %s is past the end of the buffer (%s)", position, getEnd()));
		}
		return (byte) 0;
	}

	@Override
	public int read(long position, byte[] buff, int pos, int len) {
		Arrays.fill(buff, pos, len, (byte) 0);
		return len;
	}

	@Override
	public int read(long position, ByteBuffer buff) {
		try {
			return buff.remaining();
		} finally {
			buff.position(buff.limit());
		}

	}

	@Override
	public long getLength() {
		return length;
	}

	@Override
	public long getEnd() {
		return LongSpan.calcEnd(this);
	}

}