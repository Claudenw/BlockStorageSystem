package org.xenei.blockstorage.memorymapped;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.xenei.blockstorage.MemoryMappedStorage;

public class MMFreeListTest {

	BufferFactory factory = new BufferFactory();
	MMFreeList freeList;

	@Before
	public void setup() throws IOException {
		factory.map.clear();
		freeList = new MMFreeList(factory);
	}

	@Test
	public void testConstruct() {
		assertEquals(0, freeList.blockCount());
		assertEquals(0, freeList.freeSpace());
		assertTrue(freeList.isEmpty());
		assertNull(freeList.getBlock());

		assertEquals(0, freeList.count());
		assertEquals(0, freeList.buffUsed());
		ByteBuffer bb = freeList.getBuffer();
		assertEquals(0, freeList.offset());
		LongBuffer lb = freeList.getFreeRecords();
		assertEquals(0, lb.position());
		int capacity = FreeNode.DATA_SIZE / Long.BYTES;
		assertEquals(capacity, lb.capacity());
		assertEquals(capacity, lb.limit());
	}

	@Test
	public void testAdd() throws IOException {

		Long recordNumber = 2L * MemoryMappedStorage.BLOCK_SIZE;
		freeList.add(recordNumber);

		assertEquals(1, freeList.blockCount());
		assertEquals(MemoryMappedStorage.BLOCK_SIZE, freeList.freeSpace());
		assertFalse(freeList.isEmpty());

		assertEquals(1, freeList.count());
		assertEquals(Long.BYTES, freeList.buffUsed());
		ByteBuffer bb = freeList.getBuffer();
		assertEquals(0, freeList.offset());
		LongBuffer lb = freeList.getFreeRecords();
		int capacity = FreeNode.DATA_SIZE / Long.BYTES;
		assertEquals(capacity, lb.capacity());
		assertEquals(capacity, lb.limit());
		assertEquals(recordNumber.longValue(), lb.get(0));

		assertEquals(recordNumber, freeList.getBlock());
	}

	@Test
	public void testPageExpansion() throws IOException {

		int capacity = FreeNode.DATA_SIZE / Long.BYTES;

		for (long l = 0; l < capacity; l++) {
			Long recordNumber = (2 + l) * MemoryMappedStorage.BLOCK_SIZE;
			freeList.add(recordNumber);
		}

		assertEquals(capacity, freeList.blockCount());
		assertEquals(capacity * MemoryMappedStorage.BLOCK_SIZE, freeList.freeSpace());
		assertFalse(freeList.isEmpty());

		assertEquals(capacity, freeList.count());
		assertEquals(capacity * Long.BYTES, freeList.buffUsed());
		ByteBuffer bb = freeList.getBuffer();
		assertEquals(0, freeList.offset());
		LongBuffer lb = freeList.getFreeRecords();
		assertEquals(capacity, lb.capacity());
		assertEquals(capacity, lb.limit());
		assertEquals(capacity, freeList.blockCount());
		assertEquals(0, freeList.pages.size());

		// now add one more
		Long recordNumber = (3L + capacity) * MemoryMappedStorage.BLOCK_SIZE;
		freeList.add(recordNumber);

		// check for the extra stuff.
		assertEquals(1, freeList.pages.size());
		assertEquals(capacity + 1, freeList.blockCount());

		assertEquals((capacity + 1) * MemoryMappedStorage.BLOCK_SIZE, freeList.freeSpace());
		assertFalse(freeList.isEmpty());

		assertEquals(capacity, freeList.count());
		assertEquals(capacity * Long.BYTES, freeList.buffUsed());
		bb = freeList.getBuffer();
		assertEquals(0, freeList.offset());
		lb = freeList.getFreeRecords();
		assertEquals(capacity, lb.capacity());
		assertEquals(capacity, lb.limit());
		assertEquals(capacity + 1, freeList.blockCount());
	}

	@Test
	public void testGetBlockAfterExpansion() throws IOException {

		int capacity = FreeNode.DATA_SIZE / Long.BYTES;
		Long recordNumber = null;
		for (long l = 0; l < capacity; l++) {
			recordNumber = (2 + l) * MemoryMappedStorage.BLOCK_SIZE;
			freeList.add(recordNumber);
		}

		recordNumber = (capacity + 2L) * MemoryMappedStorage.BLOCK_SIZE;
		freeList.add(recordNumber);

		// check for the extra stuff.
		assertEquals(1, freeList.pages.size());
		assertEquals(capacity + 1, freeList.blockCount());

		assertEquals((capacity + 1) * MemoryMappedStorage.BLOCK_SIZE, freeList.freeSpace());
		assertFalse(freeList.isEmpty());

		assertEquals(capacity, freeList.count());
		assertEquals(capacity * Long.BYTES, freeList.buffUsed());
		ByteBuffer bb = freeList.getBuffer();
		assertEquals(0, freeList.offset());
		LongBuffer lb = freeList.getFreeRecords();
		assertEquals(capacity, lb.capacity());
		assertEquals(capacity, lb.limit());
		assertEquals(capacity + 1, freeList.blockCount());

		// the first record should be the last one written.
		Long capNumber = (2l + capacity) * MemoryMappedStorage.BLOCK_SIZE;
		assertEquals(recordNumber, capNumber);
		Long readNumber = freeList.getBlock();
		assertEquals(recordNumber, readNumber);

		// the second one should be record 1 as the page is freed.
		readNumber = freeList.getBlock();
		assertEquals(Long.valueOf(1), readNumber);

		for (int capCount = capacity - 1; capCount >= 0; capCount--) {
//			Long capNumber = (2l+capCount) * MemoryMappedStorage.BLOCK_SIZE;
//			assertEquals( recordNumber, capNumber);
			recordNumber = Long.valueOf(recordNumber - MemoryMappedStorage.BLOCK_SIZE);
			readNumber = freeList.getBlock();
			assertEquals(recordNumber, readNumber);
		}
		assertTrue(freeList.isEmpty());
	}

	@Test
	public void testGetBlockBeforeExpansion() throws IOException {

		int capacity = FreeNode.DATA_SIZE / Long.BYTES;
		Long recordNumber = null;
		for (long l = 0; l < capacity; l++) {
			recordNumber = (2 + l) * MemoryMappedStorage.BLOCK_SIZE;
			freeList.add(recordNumber);
		}

		// check for the extra stuff.
		assertEquals(0, freeList.pages.size());

		for (long capCount = capacity - 1; capCount >= 0; capCount--) {
			Long capNumber = (2 + capCount) * MemoryMappedStorage.BLOCK_SIZE;
			assertEquals(recordNumber, capNumber);
			Long readRecord = freeList.getBlock();
			assertEquals(readRecord, recordNumber);
			recordNumber = Long.valueOf(recordNumber - MemoryMappedStorage.BLOCK_SIZE);
		}
		assertTrue(freeList.isEmpty());
	}

	private class BufferFactory implements MMFreeList.BufferFactory {

		Map<Long, ByteBuffer> map = new HashMap<Long, ByteBuffer>();

		@Override
		public ByteBuffer createBuffer() throws IOException {
			return readBuffer(map.size());
		}

		@Override
		public ByteBuffer readBuffer(long offset) throws IOException {
			ByteBuffer bb = null;
			if (map.containsKey(offset)) {
				bb = map.get(offset);
			} else {
				bb = ByteBuffer.allocate(MemoryMappedStorage.BLOCK_SIZE);
				BlockHeader header = new BlockHeader(bb);
				header.offset(offset);
				map.put(offset, bb);
			}
			return bb;
		}

	}
}
