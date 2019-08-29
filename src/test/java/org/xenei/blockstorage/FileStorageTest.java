package org.xenei.blockstorage;

import java.io.File;
import java.io.IOException;

public class FileStorageTest extends AbstractStorageTest {

	public FileStorageTest() {
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
		return new FileStorage("/tmp/storage.test");
	}
}
