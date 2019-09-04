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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.commons.io.IOUtils;
import org.xenei.blockstorage.memorymapped.BlockHeader;
import org.xenei.blockstorage.memorymapped.MMFreeList;
import org.xenei.blockstorage.memorymapped.MMOutputStream;
import org.xenei.blockstorage.memorymapped.MMPosition;
import org.xenei.blockstorage.memorymapped.MMSerde;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * A memory mapped storage implementation. Reads and write the the file via
 * memory mapped blocks.
 *
 */
public class MemoryMappedStorage implements Storage {

	public final static int BLOCK_SIZE = 2 * 1024;

	private FileChannel fileChannel;
	private MMFreeList freeList;
	private Stats stats;
	private MMSerde serde;

	/**
	 * Constructor.
	 * 
	 * @param fileName the file to process.
	 * @throws IOException on error.
	 */
	@SuppressWarnings("resource")
	public MemoryMappedStorage(String fileName) throws IOException {
		boolean clearBlock = false;
		File f = new File(fileName);

		if (!f.exists()) {
			f.createNewFile();
			clearBlock = true;
		}
		RandomAccessFile file = new RandomAccessFile(fileName, "rw");
		fileChannel = file.getChannel();
		if (clearBlock) {
			MappedByteBuffer mBuffer = fileChannel.map(MapMode.READ_WRITE, 0, BLOCK_SIZE);
			BlockHeader header = new BlockHeader(mBuffer);
			header.clear();
		}
		freeList = new MMFreeList(fileChannel);
		serde = new MMSerde(freeList, fileChannel);
		stats = new StatsImpl();
	}

	@Override
	public Stats stats() {
		return stats;
	}

	@Override
	public SpanBuffer getFirstRecord() throws IOException {
		return read(BLOCK_SIZE);
	}

	@Override
	public void setFirstRecord(SpanBuffer buffer) throws IOException {
		write(BLOCK_SIZE, buffer);
	}

	@Override
	public void write(long pos, Serializable s) throws IOException {

		try (MMOutputStream tos = new MMOutputStream(pos, serde);
				ObjectOutputStream oos = new ObjectOutputStream(tos)) {
			oos.writeObject(s);
			oos.close();
		}
	}

	@Override
	public void write(long pos, SpanBuffer spanBuffer) throws IOException {
		try (MMOutputStream tos = new MMOutputStream(pos, serde);
				ObjectOutputStream oos = new ObjectOutputStream(tos)) {
			oos.writeObject(spanBuffer.getInputStream());
			oos.close();
		}
	}

	@Override
	public long append(Serializable s) throws IOException {
		try (MMOutputStream tos = new MMOutputStream(serde); ObjectOutputStream oos = new ObjectOutputStream(tos)) {
			oos.writeObject(s);
			oos.close();
			return ((MMPosition) tos.getPosition()).offset();
		}
	}

	@Override
	public long append(SpanBuffer spanBuffer) throws IOException {
		try (MMOutputStream tos = new MMOutputStream(serde)) {
			IOUtils.copyLarge(spanBuffer.getInputStream(), tos);
			tos.close();
			return ((MMPosition) tos.getPosition()).offset();
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
		return Factory.wrap(serde.getDeserializer().deserialize(new MMPosition(offset)));
	}

	@Override
	public void delete(long offset) throws IOException {
		serde.delete(new MMPosition(offset));
	}

	@Override
	public void close() throws IOException {
		fileChannel.close();
		freeList = null;
	}

	/**
	 * Stats implementation foe MemoryMappedStorage.
	 *
	 */
	public class StatsImpl implements Stats {

		@Override
		public long dataLength() {
			try {
				return fileChannel.size();
			} catch (IOException e) {
				return -1;
			}
		}

		@Override
		public long deletedBlocks() {
			return (freeList == null) ? -1 : freeList.blockCount();
		}

		@Override
		public long freeSpace() {
			return (freeList == null) ? -1 : freeList.freeSpace();
		}

		@Override
		public String toString() {
			return String.format("l:%s f:%s d:%s", dataLength(), freeSpace(), deletedBlocks());
		}
	}

}
