package org.xenei.blockstorage;

import java.io.IOException;
import java.util.Arrays;

import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.AbstractSpanBuffer;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * A span buffer that returns a specified number of null bytes (0)
 *
 */
class FillBuffer extends AbstractSpanBuffer {

	long length;

	public FillBuffer(long length) {
		super(0);
		this.length = length;
	}

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
			throw new IOException(
					String.format("Position %s is past the end of the buffer (%s)", position, getEnd()));
		}
		return (byte) 0;
	}

	@Override
	public int read(long position, byte[] buff, int pos, int len) {
		Arrays.fill(buff, pos, len, (byte) 0);
		return len;
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