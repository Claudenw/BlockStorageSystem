package org.xenei.blockstorage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

public class FileStorageTest extends AbstractStorageTest {
	
	public Storage createStorage () throws IOException{
		return new FileStorage( "/tmp/storage.test");
	}
}
