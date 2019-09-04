package org.xenei.blockstorage.memorymapped;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.junit.Test;
import org.xenei.blockstorage.MemoryMappedStorage;

public class FreeNodeTest {
	FreeNode freeNode;

	public FreeNodeTest() throws IOException {
		freeNode = new FreeNode(ByteBuffer.allocate(MemoryMappedStorage.BLOCK_SIZE));
	}

	@Test
	public void testDataReadWrite() {
		LongBuffer lb = freeNode.getFreeRecords();

		for (int i = 0; i < 10; i++) {
			lb.put(i, i);
		}
		for (int i = 0; i < 10; i++) {
			assertEquals(i, lb.get(i));
		}
	}

}
