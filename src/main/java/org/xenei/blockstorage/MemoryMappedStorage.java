package org.xenei.blockstorage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.RandomAccess;

import org.apache.commons.io.IOUtils;
import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.streams.SpanBufferOutputStream;

public class MemoryMappedStorage implements Storage {

	private final static int DEFAULT_BLOCK_SIZE = 2 * 1024;

	private FileChannel fileChannel;
	private FreeBuffer freeBuffer;
	private Stats stats;

	public MemoryMappedStorage(String fileName) throws IOException {

		File f = new File(fileName);
		RandomAccessFile file = null;
		BlockHeader header = null;
		if (f.exists()) {
			file = new RandomAccessFile(fileName, "rw");
			fileChannel = file.getChannel();
			header = new BlockHeader( 0 );
		} else {
			f.createNewFile();
			file = new RandomAccessFile(fileName, "rw");
			fileChannel = file.getChannel();
			header = new BlockHeader(fileChannel);
		}
		
		freeBuffer = new FreeBuffer(header.getSpanBuffer());

		stats = new StatsImpl();
	}

	public Stats stats() {
		return stats;
	}

	public SpanBuffer getFirstRecord() throws IOException {
		return read(DEFAULT_BLOCK_SIZE);
	}

	public void setFirstRecord(SpanBuffer buffer) throws IOException {
		write(DEFAULT_BLOCK_SIZE, buffer);
	}

	private void writeFreeBlocks() throws IOException {
		BlockHeader header = new BlockHeader(0);
		
		InputStream in = freeBuffer.getInputStream();
		FreeInfo freeInfo = new FreeInfo();
		freeInfo.useFreeList = false;
		freeInfo.skipFlush = true;
		header.write(in, freeInfo );
	}

	public void write(long pos, Serializable s) throws IOException {
		SpanBufferOutputStream sbos = new SpanBufferOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(sbos)) {
			oos.writeObject(s);
		}
		write(pos, sbos.getSpanBuffer());
	}

	public void write(long pos, SpanBuffer buff) throws IOException {
		BlockHeader header = new BlockHeader(pos);
		header.write( buff.getInputStream(), new FreeInfo() );
	}

	public long append(Serializable s) throws IOException {
		SpanBufferOutputStream sbos = new SpanBufferOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(sbos)) {
			oos.writeObject(s);
		}
		return append(sbos.getSpanBuffer());
	}

	public long append(SpanBuffer buff) throws IOException {
		FreeInfo freeInfo = new FreeInfo();
		BlockHeader nextHeader = null;
		if ( ! freeBuffer.isEmpty()) {
			synchronized (freeBuffer) {	
				freeInfo.needsFlush = true;
				nextHeader = new BlockHeader( freeBuffer.getBlock() );
			}
		}		
		else
		{
			nextHeader = new BlockHeader( fileChannel );
		}

		nextHeader.write( buff.getInputStream(), freeInfo );
		return nextHeader.blockInfo.getOffset();
	}

	public Serializable readObject(long pos) throws IOException, ClassNotFoundException {
		try (ObjectInputStream ois = new ObjectInputStream(read(pos).getInputStream())) {
			return (Serializable) ois.readObject();
		}
	}

	public SpanBuffer read(long offset) throws IOException {		
		BlockHeader header = new BlockHeader( offset );
		return header.getSpanBuffer();
	}

	public void delete(long offset) throws IOException {
		BlockHeader header = new BlockHeader(offset);
		freeBuffer.add(header.blockInfo);
		while (header.nextBlock != 0) {
			header = new BlockHeader(header.nextBlock);
			freeBuffer.add(header.blockInfo);
		}
		writeFreeBlocks();
	}

	public void close() throws IOException {
		fileChannel.close();
		freeBuffer = null;

	}

	private class BlockHeader {
		public static final int HEADER_SIZE = 3 * Long.BYTES;
		private MappedByteBuffer buffer;
		private LongSpan blockInfo;
		private long buffUsed;
		private long nextBlock;
		
		public BlockHeader( FileChannel fileChannel) throws IOException {
			long offset = fileChannel.size();
			MappedByteBuffer buff = fileChannel.map( FileChannel.MapMode.READ_WRITE, offset,  DEFAULT_BLOCK_SIZE );
			buff.position(0);
			buff.putLong( DEFAULT_BLOCK_SIZE );
			buff.putLong( DEFAULT_BLOCK_SIZE );
			blockInfo = LongSpan.fromLength( offset, DEFAULT_BLOCK_SIZE);
			nextBlock = 0;
			buffUsed = DEFAULT_BLOCK_SIZE;
			clear();			
		}
		
		public BlockHeader( LongSpan span ) throws IOException {
			this( span.getOffset(), fileChannel.map( FileChannel.MapMode.READ_WRITE, span.getOffset(),  span.getLength() ));
		}
		
		public BlockHeader( long offset ) throws IOException 
		{
			this( offset, fileChannel.map( FileChannel.MapMode.READ_WRITE, offset,  DEFAULT_BLOCK_SIZE ));
		}
		
		public BlockHeader( long offset, MappedByteBuffer buffer )
		{
			blockInfo = LongSpan.fromLength( offset, buffer.getLong() );
			buffUsed = buffer.getLong();
			nextBlock = buffer.getLong();
		}
		
		public void write(RandomAccessFile file) throws IOException {
			file.seek(blockInfo.getOffset());
			file.writeLong(blockInfo.getLength());
			file.writeLong(buffUsed);
			file.writeLong(nextBlock);
		}
	
		public LongSpan getDataSpan() {
			return LongSpan.fromEnd(blockInfo.getOffset() + HEADER_SIZE, blockInfo.getEnd());
		}
		
		public void setNextBlock( long nextBlock )
		{
			buffer.putLong( Long.BYTES*2, nextBlock );
			this.nextBlock = nextBlock;
		}
		
		public SpanBuffer getSpanBuffer() throws IOException {		
			List<SpanBuffer> lst = new ArrayList<SpanBuffer>();
			SpanBuffer sb = Factory.wrap( buffer ).cut( HEADER_SIZE );
			if (sb.getLength() > buffUsed) {
				sb = sb.head(buffUsed);
			}
			lst.add(sb);
			if (sb.getLength() < buffUsed ) {
				long len = buffUsed - sb.getLength();
				lst.add( Factory.wrap(fileChannel.map( FileChannel.MapMode.READ_WRITE, sb.getEnd()+1,  len )));
			}
			
			if (nextBlock != 0)
			{
				BlockHeader nxtHeader = new BlockHeader( nextBlock );
				lst.add( nxtHeader.getSpanBuffer() );
			}
			return Factory.merge( lst.iterator() );
		}
		
		private void write(InputStream in, FreeInfo freeInfo) throws IOException {
			boolean doFlush = ! freeInfo.skipFlush;
			long limit = Long.min(blockInfo.getLength(), buffer.capacity());
			buffer.position( HEADER_SIZE );
			BufferOutputStream bos = new BufferOutputStream(buffer);
			IOUtils.copyLarge(in, bos, 0, limit );
			if (in.available() > 0)
			{
				if (buffer.capacity() < blockInfo.getLength() ) {
					long len = buffUsed - buffer.capacity();
					long pos = blockInfo.getOffset()+buffer.capacity();
					bos = new BufferOutputStream(
							fileChannel.map( FileChannel.MapMode.READ_WRITE, pos,  len ));
					IOUtils.copyLarge(in, bos, 0, len );
				}	
			}
			if (in.available() > 0) {
				BlockHeader nextHeader = null;
				freeInfo.skipFlush = true;
				if (nextBlock == 0)
				{
					if (freeInfo.useFreeList && ! freeBuffer.isEmpty()) {
						synchronized (freeBuffer) {	
							freeInfo.needsFlush = true;
							nextHeader = new BlockHeader( freeBuffer.getBlock() );
						}
					}		
					else
					{
						nextHeader = new BlockHeader( fileChannel );
					}

				}
				setNextBlock( nextHeader.blockInfo.getOffset() );
				nextHeader.write(in, freeInfo );				
			} else {
				if (nextBlock != 0) {
					// remove extra blocks.
					if (freeInfo.useFreeList)
					{
						delete( nextBlock );
					} else {
						long theBlock = nextBlock;
						while (theBlock != 0)
						{
							BlockHeader nextHeader = new BlockHeader( theBlock );
							nextHeader.clear();
							theBlock = nextHeader.nextBlock;
						}
						
					}
				}
			}
			if (freeInfo.needsFlush && doFlush)
			{
				writeFreeBlocks();
			}
		}
		
		private void clear() {
			buffer.position( HEADER_SIZE );
			buffer.put( (int)blockInfo.getLength()-HEADER_SIZE , (byte)0);
		}
	}
	
	/**
	 * Class to relay info about free list usage and demands during writing.
	 *
	 * Default values are normal for standard writes.
	 */
	private class FreeInfo {
		/**
		 * If true use the free list (set false when writing the free list)
		 */
		boolean useFreeList = true;
		/**
		 * skip flushing the free list. (only valid when useFreeList is active)
		 */
		boolean skipFlush = false;
		/**
		 * set true if the system needs to flush the free list.
		 */
		boolean needsFlush = false;
	}

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
	
	private class BufferOutputStream extends OutputStream {
		
		private MappedByteBuffer buffer;
		
		BufferOutputStream( MappedByteBuffer buffer )
		{
			this.buffer = buffer;
		}

		@Override
		public void write(int b) throws IOException {
			this.buffer.put((byte)b);
		}

		@Override
		public void write(byte[] b, int s, int l) throws IOException {
			this.buffer.put( b, s, l );
		}

		@Override
		public void write(byte[] b) throws IOException {
			this.buffer.put( b );
		}
		
		
		
	}
}
