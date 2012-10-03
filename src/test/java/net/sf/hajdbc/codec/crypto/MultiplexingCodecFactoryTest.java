package net.sf.hajdbc.codec.crypto;

import java.sql.SQLException;

import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.codec.DecoderFactory;
import net.sf.hajdbc.codec.MultiplexingDecoderFactory;

import org.junit.Assert;
import org.junit.Test;

public class MultiplexingCodecFactoryTest extends CipherCodecFactoryTest
{
	private final DecoderFactory factory = new MultiplexingDecoderFactory();
	
	@Test
	public void decodeSimple() throws SQLException
	{
		Decoder codec = this.factory.createDecoder(null);
		
		Assert.assertEquals("password", codec.decode("password"));
	}
	
	@Test
	public void decodeHex() throws SQLException
	{
		Decoder codec = this.factory.createDecoder(null);
		
		Assert.assertEquals("password", codec.decode("16:70617373776f7264"));
	}
	
	@Test
	public void decodeBase64() throws SQLException
	{
		Decoder codec = this.factory.createDecoder(null);
		
		Assert.assertEquals("password", codec.decode("64:cGFzc3dvcmQ="));
	}
	
	@Test
	public void test() throws SQLException
	{
		Decoder codec = this.factory.createDecoder(null);
		
		Assert.assertEquals("password", codec.decode("?:wzAkF0hlYUeGhfzRQIxYAQ=="));
	}
}
