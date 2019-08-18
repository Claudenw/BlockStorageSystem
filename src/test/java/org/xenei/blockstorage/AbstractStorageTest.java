package org.xenei.blockstorage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

public abstract class AbstractStorageTest {

	Storage storage;
	
	
	@Before
	public void setup() throws IOException
	{
		storage = createStorage();
	}
	
	abstract Storage createStorage() throws IOException;
	
	@Test
	public void test() throws IOException {
		
		System.out.println( storage.stats() );
		
		long first = storage.append( Factory.wrap( "Hello world" ));
		System.out.println( storage.stats() );

		long second = storage.append( Factory.wrap( "Goodbye cruel world" ));
		System.out.println( storage.stats() );

		SpanBuffer f = storage.read( first );
		assertEquals( "Hello world", f.getText());
		System.out.println( storage.stats() );
		
		SpanBuffer s = storage.read( second );
		assertEquals( "Goodbye cruel world", s.getText());
		System.out.println( storage.stats() );
		
		storage.delete( first );
		System.out.println( storage.stats() );
		
		long third = storage.append( Factory.wrap( "Hello again"));
		assertEquals( first, third );
		System.out.println( storage.stats() );
		
		storage.close();
		System.out.println( storage.stats() );
		
		storage = createStorage();
		System.out.println( storage.stats() );

		f = storage.read( first );
		assertEquals( "Hello again", f.getText());
		System.out.println( storage.stats() );
		
		s = storage.read( second );
		assertEquals( "Goodbye cruel world", s.getText());
		System.out.println( storage.stats() );
		
		storage.close();
				
	}
}
