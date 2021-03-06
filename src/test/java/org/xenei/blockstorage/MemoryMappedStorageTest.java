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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.xenei.blockstorage.memorymapped.BlockHeader;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

public class MemoryMappedStorageTest extends AbstractStorageTest {

	public MemoryMappedStorageTest() {
		File f = new File("/tmp/storage.test");
		if (f.exists()) {
			f.delete();
		}

	}

	@Override
	public Storage createStorage() throws IOException {
		return new MemoryMappedStorage("/tmp/storage.test");
	}

	@Test
	public void test() throws IOException {

		assertEquals(2048, storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		long first = storage.append(Factory.wrap("Hello world"));
		assertEquals(8192, storage.stats().dataLength());
		assertEquals(4096, storage.stats().freeSpace());
		assertEquals(2, storage.stats().deletedBlocks());

		long second = storage.append(Factory.wrap("Goodbye cruel world"));
		assertEquals(10240, storage.stats().dataLength());
		assertEquals(4096, storage.stats().freeSpace());
		assertEquals(2, storage.stats().deletedBlocks());

		SpanBuffer f = storage.read(first);
		assertEquals("Hello world", f.getText());
		assertEquals(10240, storage.stats().dataLength());
		assertEquals(4096, storage.stats().freeSpace());
		assertEquals(2, storage.stats().deletedBlocks());

		SpanBuffer s = storage.read(second);
		assertEquals("Goodbye cruel world", s.getText());
		assertEquals(10240, storage.stats().dataLength());
		assertEquals(4096, storage.stats().freeSpace());
		assertEquals(2, storage.stats().deletedBlocks());

		storage.delete(first);
		assertEquals(10240, storage.stats().dataLength());
		assertEquals(6144, storage.stats().freeSpace());
		assertEquals(3, storage.stats().deletedBlocks());

		try {
			storage.read(first).getText();
			fail("Should have thrown IOException");
		} catch (IOException expected) {
			// do nothign.
		}

		long third = storage.append(Factory.wrap("Hello again"));
		assertEquals(4096, third);
		assertEquals(10240, storage.stats().dataLength());
		assertEquals(4096, storage.stats().freeSpace());
		assertEquals(2, storage.stats().deletedBlocks());

		storage.close();
		assertEquals(-1, storage.stats().dataLength());
		assertEquals(-1, storage.stats().freeSpace());
		assertEquals(-1, storage.stats().deletedBlocks());

		storage = createStorage();
		assertEquals(10240, storage.stats().dataLength());
		assertEquals(4096, storage.stats().freeSpace());
		assertEquals(2, storage.stats().deletedBlocks());

		f = storage.read(third);
		assertEquals("Hello again", f.getText());
		assertEquals(10240, storage.stats().dataLength());
		assertEquals(4096, storage.stats().freeSpace());
		assertEquals(2, storage.stats().deletedBlocks());

		s = storage.read(second);
		assertEquals("Goodbye cruel world", s.getText());
		assertEquals(10240, storage.stats().dataLength());
		assertEquals(4096, storage.stats().freeSpace());
		assertEquals(2, storage.stats().deletedBlocks());

		storage.close();
		assertEquals(-1, storage.stats().dataLength());
		assertEquals(-1, storage.stats().freeSpace());
		assertEquals(-1, storage.stats().deletedBlocks());

	}

	/**
	 * An application that verifies the headers in a file.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		RandomAccessFile file = new RandomAccessFile("/tmp/storage.test", "r");
		if ((file.length() % MemoryMappedStorage.BLOCK_SIZE) != 0) {
			System.err.println("Not a proper block sized file");
			System.exit(1);
		}
		byte[] buff = new byte[BlockHeader.HEADER_SIZE];
		BlockHeader header = new BlockHeader(ByteBuffer.wrap(buff));
		int limit = (int) file.length() / MemoryMappedStorage.BLOCK_SIZE;
		for (int i = 0; i < limit; i++) {
			file.seek(i * MemoryMappedStorage.BLOCK_SIZE);
			file.read(buff);
			System.out.println(String.format("%s: %s", i, header));
			try {
				header.verify(i * MemoryMappedStorage.BLOCK_SIZE);
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
		}
	}
}
