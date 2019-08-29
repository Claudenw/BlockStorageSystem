package org.xenei.blockstorage;

import java.io.File;
import java.io.IOException;

public class MemoryMappedStorageTest extends AbstractStorageTest {

	public MemoryMappedStorageTest() {
		File f = new File("/tmp/storage.test");
		if (f.exists()) {
			f.delete();
		}

	}
	
	@Override
	public int getEmptySize() {
		 return 2048;
	}

	@Override
	public Storage createStorage() throws IOException {
		return new MemoryMappedStorage("/tmp/storage.test");
	}
}
