package org.xenei.blockstorage;

import java.io.IOException;
import java.io.Serializable;
import org.xenei.spanbuffer.SpanBuffer;

public interface Storage {

	public final static int DEFAULT_BLOCK_SIZE = 2 * 1024;

	public Stats stats();

	public SpanBuffer getFirstRecord() throws IOException;

	public void setFirstRecord(SpanBuffer buffer) throws IOException;	

	public void write(long pos, Serializable s) throws IOException;

	public void write(long pos, SpanBuffer buff) throws IOException;

	public long append(Serializable s) throws IOException;

	public long append(SpanBuffer buff) throws IOException;
	
	public Serializable readObject(long pos) throws IOException, ClassNotFoundException;

	public SpanBuffer read(long offset) throws IOException;
	
	public void delete(long offset) throws IOException;

	public void close() throws IOException;
	
}
