package org.xenei.blockstorage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

public abstract class AbstractStorageTest {

	private Storage storage;
	
	

	@Before
	public void setup() throws IOException {
		System.setProperty( "org.slf4j.simpleLogger.defaultLogLevel", "debug" );
		storage = createStorage();
	}
	
	abstract int getEmptySize();

	abstract Storage createStorage() throws IOException;

	@Test
	public void test() throws IOException {

		assertEquals(getEmptySize(), storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		long first = storage.append(Factory.wrap("Hello world"));
//		assertEquals(4096, storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		long second = storage.append(Factory.wrap("Goodbye cruel world"));
//		assertEquals(6144, storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		SpanBuffer f = storage.read(first);
		assertEquals("Hello world", f.getText());
//		assertEquals(6144, storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		SpanBuffer s = storage.read(second);
		assertEquals("Goodbye cruel world", s.getText());
//		assertEquals(6144, storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		storage.delete(first);
//		assertEquals(6144, storage.stats().dataLength());
		assertEquals(2048, storage.stats().freeSpace());
		assertEquals(1, storage.stats().deletedBlocks());

		long third = storage.append(Factory.wrap("Hello again"));
		assertEquals(first, third);
//		assertEquals(6144, storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		storage.close();
		assertEquals(-1, storage.stats().dataLength());
		assertEquals(-1, storage.stats().freeSpace());
		assertEquals(-1, storage.stats().deletedBlocks());

		storage = createStorage();
//		assertEquals(6144, storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		f = storage.read(first);
		assertEquals("Hello again", f.getText());
//		assertEquals(6144, storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		s = storage.read(second);
		assertEquals("Goodbye cruel world", s.getText());
//		assertEquals(6144, storage.stats().dataLength());
		assertEquals(0, storage.stats().freeSpace());
		assertEquals(0, storage.stats().deletedBlocks());

		storage.close();
		assertEquals(-1, storage.stats().dataLength());
		assertEquals(-1, storage.stats().freeSpace());
		assertEquals(-1, storage.stats().deletedBlocks());

	}
}
