package net.sf.hajdbc.sql.io;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.durability.Durability.Phase;

import org.junit.Test;
import org.mockito.Mockito;

public class IOExceptionFactoryTest
{
	private final ExceptionFactory<IOException> factory = new IOExceptionFactory();
	
	@Test
	public void createExceptionFromMessage()
	{
		String message = "message";
		
		IOException result = this.factory.createException(message);
		
		assertSame(message, result.getMessage());
		assertNull(result.getCause());
	}
	
	@Test
	public void createExceptionFromException()
	{
		String message = "message";
		Exception exception = new Exception(message);
		
		IOException result = this.factory.createException(exception);
		
		assertNotNull(result.getMessage());
		assertSame(exception.getMessage(), result.getMessage());
		assertSame(exception, result.getCause());
	}
	
	@Test
	public void createExceptionFromSQLException()
	{
		IOException exception = new IOException();
		
		IOException result = this.factory.createException(exception);
		
		assertSame(exception, result);
	}
	
	@Test
	public void getType()
	{
		ExceptionType result = this.factory.getType();
		
		assertSame(ExceptionType.IO, result);
	}
	
	@Test
	public void equals()
	{
		assertTrue(this.factory.equals(new IOException(), new IOException()));
		assertFalse(this.factory.equals(new IOException("message"), new IOException()));
		assertTrue(this.factory.equals(new IOException("message"), new IOException("message")));
		assertFalse(this.factory.equals(new IOException("message1"), new IOException("message2")));
	}
	
	@Test
	public void correctHeuristic()
	{
		assertFalse(this.factory.correctHeuristic(new IOException(), Phase.COMMIT));
		assertFalse(this.factory.correctHeuristic(new IOException(), Phase.FORGET));
		assertFalse(this.factory.correctHeuristic(new IOException(), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new IOException(), Phase.ROLLBACK));
	}
	
	@Test
	public void indicatesFailure()
	{
		Dialect dialect = mock(Dialect.class);
		
		assertFalse(this.factory.indicatesFailure(new IOException(), dialect));

		Mockito.verifyNoMoreInteractions(dialect);
	}
}
