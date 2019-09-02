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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.Walker;
import org.xenei.spanbuffer.streams.SpanBufferOutputStream;

/**
 * A file storage system that reads/write from a file directly.
 *
 */
public class FileStorage implements Storage {

	private final static int DEFAULT_BLOCK_SIZE = 2 * 1024;

	private RandomAccessFile file;

	/**
	 * The free blocks in a SpanBuffer structure with block add and remove
	 * functionality.
	 */
	private FreeBuffer freeBuffer;
	/**
	 * An output stream that always writes at the current file location.
	 */
	private OutputStream fileStream;
	private Stats stats;

	/**
	 * Constructor
	 * 
	 * @param fileName The name of the file to process
	 * @throws IOException on error
	 */
	public FileStorage(String fileName) throws IOException {
		fileStream = new OutputStream() {

			@Override
			public void write(int b) throws IOException {
				file.write(b);
			}

			@Override
			public void write(byte[] b) throws IOException {
				file.write(b);
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				file.write(b, off, len);
			}
		};

		File f = new File(fileName);
		if (f.exists()) {
			file = new RandomAccessFile(fileName, "rw");
			// build the span buffer
			List<SpanBuffer> sbList = new ArrayList<SpanBuffer>();
			BlockHeader header = new BlockHeader();
			header.read(0);
			InputStream is = new LimitedInputStream((int) header.getDataSpan().getLength());
			sbList.add(Factory.wrap(is));
			while (header.nextBlock != 0) {
				header.read(header.nextBlock);
				is = new LimitedInputStream((int) header.buffUsed);
				sbList.add(Factory.wrap(is));
			}
			freeBuffer = new FreeBuffer(Factory.merge(sbList.iterator()));

		} else {
			f.createNewFile();
			file = new RandomAccessFile(fileName, "rw");
			BlockHeader header = new BlockHeader();
			header.blockInfo = LongSpan.fromLength(0, DEFAULT_BLOCK_SIZE);
			header.nextBlock = 0;
			header.buffUsed = header.getDataSpan().getLength();
			header.write();
			IOUtils.copy(new FillBuffer(header.getDataSpan().getLength()).getInputStream(), fileStream);
			freeBuffer = new FreeBuffer();
		}
		stats = new StatsImpl();
	}

	@Override
	public Stats stats() {
		return stats;
	}

	@Override
	public SpanBuffer getFirstRecord() throws IOException {
		return read(DEFAULT_BLOCK_SIZE);
	}

	@Override
	public void setFirstRecord(SpanBuffer buffer) throws IOException {
		write(DEFAULT_BLOCK_SIZE, buffer);
	}

	/**
	 * copies data from a walker into the file stream.
	 * 
	 * @param walker the walker to read from.
	 * @param len    the maximum number of bytes to write.
	 * @param buffer the buffer to write with.
	 * @return the number of bytes writen.
	 * @throws IOException
	 */
	private void fillBlock(Walker walker, long len, byte[] buffer) throws IOException {
		long bytesCopied = 0;
		int limit = (int) Long.min(len, buffer.length);
		int bytesRead = 0;
		while (walker.hasCurrent() && limit > 0) {
			if (limit > buffer.length) {
				bytesRead = walker.read(buffer);
			} else {
				bytesRead = walker.read(buffer, 0, limit);
			}
			fileStream.write(buffer, 0, bytesRead);
			bytesCopied += bytesRead;
		}
		if (bytesCopied < len) {
			try (InputStream in = new FillBuffer(len - bytesCopied).getInputStream()) {
				IOUtils.copyLarge(in, fileStream, buffer);
			}
		}

	}

	/**
	 * write the free blocks to the file.
	 */
	private void writeFreeBlocks() throws IOException {
		// create a buffer to copy with.
		byte[] buffer = new byte[DEFAULT_BLOCK_SIZE];
		BlockHeader header = new BlockHeader();
		// Read the first block and position file stream.
		header.read(0);
		// file stream is now positiond at header data block
		Walker walker = freeBuffer.getWalker();
		fillBlock(walker, header.getDataSpan().getLength(), buffer);

		/* The first block is now full so write any remaining data */

		while (walker.hasCurrent()) {
			if (header.nextBlock != 0) {
				header.read(header.nextBlock);
				fillBlock(walker, header.getDataSpan().getLength(), buffer);
			} else {
				header.nextBlock = file.length();
				header.write();
				header.blockInfo = LongSpan.fromLength(header.nextBlock, walker.remaining() + BlockHeader.HEADER_SIZE);
				header.nextBlock = 0;
				header.buffUsed = walker.remaining();
				header.write();
				fillBlock(walker, header.getDataSpan().getLength(), buffer);
			}
		}

		/* clear any remaining extra blocks */
		while (header.nextBlock != 0) {
			header.read(header.nextBlock);
			header.buffUsed = 0;
			header.write();
			try (InputStream is = new FillBuffer(header.getDataSpan().getLength()).getInputStream()) {
				IOUtils.copyLarge(is, fileStream);
			}
		}

	}

	/**
	 * Write the span buffer into free space.
	 * 
	 * @param buff the buffer to write.
	 * @return position of the write.
	 * @throws IOException
	 */
	private long freeWrite(SpanBuffer buff) throws IOException {
		SpanBuffer remaining = buff;
		LongSpan firstBlock = null;
		BlockHeader header = new BlockHeader();
		synchronized (freeBuffer) {
			firstBlock = freeBuffer.getBlock();
			header.blockInfo = firstBlock;
			remaining = write(remaining, header, null);
			LongSpan lastBlock = firstBlock;
			while (remaining.getLength() > 0) {
				LongSpan nextBlock = freeBuffer.getBlock();
				if (nextBlock == null) {
					nextBlock = LongSpan.fromLength(file.length(), calcBufferLen(remaining));
				}
				header.blockInfo = nextBlock;

				remaining = write(remaining, header, lastBlock);
				lastBlock = nextBlock;
			}
			writeFreeBlocks();
		}
		return firstBlock.getOffset();
	}

	private long calcBufferLen(SpanBuffer buff) {
		return Long.max(buff.getLength() + BlockHeader.HEADER_SIZE, DEFAULT_BLOCK_SIZE);
	}

	@Override
	public void write(long pos, Serializable s) throws IOException {
		SpanBufferOutputStream sbos = new SpanBufferOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(sbos)) {
			oos.writeObject(s);
		}
		write(pos, sbos.getSpanBuffer());
	}

	@Override
	public void write(long pos, SpanBuffer buff) throws IOException {
		BlockHeader header = new BlockHeader();
		header.read(pos);
		write(buff, header, null);
	}

	@Override
	public long append(Serializable s) throws IOException {
		SpanBufferOutputStream sbos = new SpanBufferOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(sbos)) {
			oos.writeObject(s);
		}
		return append(sbos.getSpanBuffer());
	}

	@Override
	public long append(SpanBuffer buff) throws IOException {
		if (!freeBuffer.isEmpty()) {
			return freeWrite(buff);
		}

		BlockHeader header = new BlockHeader();
		header.blockInfo = LongSpan.fromLength(file.length(), calcBufferLen(buff));
		write(buff, header, null);
		return header.blockInfo.getOffset();
	}

	/**
	 * Write the spanbuffer into the block data space. If lastblock is not null, set
	 * the lastBlock to point to the nextblock.
	 * 
	 * @param buff      The buffer to write
	 * @param nextBlock the block to write into.
	 * @param lastBlock the block that sould point ot he next one.
	 * @return The remaining buffer after the write.
	 * @throws IOException on error
	 */
	private SpanBuffer write(SpanBuffer buff, BlockHeader headerToWrite, LongSpan lastBlock) throws IOException {

		if (lastBlock != null) {
			BlockHeader header = new BlockHeader();
			header.read(lastBlock.getOffset());
			header.nextBlock = headerToWrite.blockInfo.getOffset();
			header.write();
		}

		headerToWrite.buffUsed = Long.min(headerToWrite.getDataSpan().getLength(), buff.getLength());
		headerToWrite.write();
		if (headerToWrite.buffUsed < buff.getLength()) {
			IOUtils.copy(buff.head(headerToWrite.buffUsed).getInputStream(), fileStream);
			SpanBuffer retval = buff.cut(headerToWrite.buffUsed);
			if (headerToWrite.nextBlock != 0) {
				if (retval.getLength() == 0) {
					delete(headerToWrite.nextBlock);
					headerToWrite.nextBlock = 0;
					headerToWrite.write();
					return retval;
				} else {
					BlockHeader header = new BlockHeader();
					header.read(headerToWrite.nextBlock);
					return write(retval, header, null);
				}
			}
			return retval;
		} else {
			IOUtils.copy(buff.getInputStream(), fileStream);
			if (headerToWrite.buffUsed < headerToWrite.getDataSpan().getLength()) {
				IOUtils.copy(new FillBuffer(headerToWrite.getDataSpan().getLength() - headerToWrite.buffUsed)
						.getInputStream(), fileStream);
			}
			if (headerToWrite.nextBlock != 0) {
				delete(headerToWrite.nextBlock);
				headerToWrite.nextBlock = 0;
				headerToWrite.write();
			}
			return Factory.EMPTY;
		}
	}

	@Override
	public Serializable readObject(long pos) throws IOException, ClassNotFoundException {
		try (ObjectInputStream ois = new ObjectInputStream(read(pos).getInputStream())) {
			return (Serializable) ois.readObject();
		}
	}

	@Override
	public SpanBuffer read(long offset) throws IOException {
		BlockHeader header = new BlockHeader();
		header.read(offset);
		byte[] buff = new byte[(int) header.buffUsed];
		file.read(buff);
		List<SpanBuffer> sb = new ArrayList<SpanBuffer>();

		sb.add(Factory.wrap(buff));
		while (header.nextBlock != 0) {
			header.read(header.nextBlock);
			long buffLen = header.buffUsed;
			while (buffLen > Integer.MAX_VALUE) {
				buff = new byte[Integer.MAX_VALUE];
				file.read(buff);
				sb.add(Factory.wrap(buff));
				buffLen -= Integer.MAX_VALUE;
			}
			if (buffLen > 0) {
				buff = new byte[Integer.MAX_VALUE];
				file.read(buff);
				sb.add(Factory.wrap(buff));
			}
		}
		return (sb.size() == 1) ? sb.get(0) : Factory.merge(sb.stream());
	}

	@Override
	public void delete(long offset) throws IOException {
		BlockHeader header = new BlockHeader();
		header.read(offset);
		freeBuffer.add(header.blockInfo);
		while (header.nextBlock != 0) {
			header.read(header.nextBlock);
			freeBuffer.add(header.blockInfo);
		}
		writeFreeBlocks();
	}

	@Override
	public void close() throws IOException {
		file.close();
		freeBuffer = null;

	}

	/**
	 * A block header. Each block has a header as the first set of data.
	 *
	 */
	private class BlockHeader {
		public static final int HEADER_SIZE = 3 * Long.BYTES;
		private LongSpan blockInfo;
		private long buffUsed;
		private long nextBlock;

		/**
		 * Reat the header at the specified offset.
		 * 
		 * @param offset the offset to read the header from.
		 * @throws IOException on error.
		 */
		public void read(long offset) throws IOException {
			file.seek(offset);
			blockInfo = LongSpan.fromLength(offset, file.readLong());
			buffUsed = file.readLong();
			nextBlock = file.readLong();
		}

		/**
		 * Write the block back to the file.
		 * 
		 * @throws IOException on error.
		 */
		public void write() throws IOException {
			file.seek(blockInfo.getOffset());
			file.writeLong(blockInfo.getLength());
			file.writeLong(buffUsed);
			file.writeLong(nextBlock);
		}

		/**
		 * Get the span of the data in this block. This is the block location and length
		 * minus the header.
		 * 
		 * @return the span for the data in this block.
		 */
		public LongSpan getDataSpan() {
			return LongSpan.fromEnd(blockInfo.getOffset() + HEADER_SIZE, blockInfo.getEnd());
		}
	}

	/**
	 * An input stream that is limited to a specific number of bytes.
	 */
	private class LimitedInputStream extends InputStream {

		private int limit;

		/**
		 * Create a limited input stream
		 * 
		 * @param limit the limit
		 */
		public LimitedInputStream(int limit) {
			this.limit = limit;
		}

		@Override
		public int read() throws IOException {
			if (limit <= 0) {
				return -1;
			} else {
				limit--;
				return file.read();
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (limit <= 0) {
				return -1;
			}
			int read = Integer.min(limit, len);
			limit -= read;
			file.read(b, off, read);
			return read;
		}

		@Override
		public int available() throws IOException {
			return limit;
		}
	}

	/**
	 * Stats implementation for FileStorage.
	 */
	public class StatsImpl implements Stats {

		@Override
		public long dataLength() {
			try {
				return file.length();
			} catch (IOException e) {
				return -1;
			}
		}

		@Override
		public long deletedBlocks() {
			return (freeBuffer == null) ? -1 : freeBuffer.getBlockCount();
		}

		@Override
		public long freeSpace() {
			return (freeBuffer == null) ? -1 : freeBuffer.getFreeSpace();
		}

		@Override
		public String toString() {
			return String.format("l:%s f:%s d:%s", dataLength(), freeSpace(), deletedBlocks());
		}
	}
}
