package net.sf.hajdbc.codec;

import java.sql.SQLException;

import net.sf.hajdbc.codec.crypto.CipherCodecFactoryTest;

import org.junit.Assert;
import org.junit.Test;

public class MultiplexingCodecFactoryTest extends CipherCodecFactoryTest
{
	private final CodecFactory factory = new MultiplexingCodecFactory();
	
	@Test
	public void decodeSimple() throws SQLException
	{
		Codec codec = this.factory.createCodec(null);
		
		Assert.assertEquals("password", codec.decode("password"));
	}
	
	@Test
	public void decodeHex() throws SQLException
	{
		Codec codec = this.factory.createCodec(null);
		
		Assert.assertEquals("password", codec.decode("H:70617373776f7264"));
	}
	
	@Test
	public void decodeBase64() throws SQLException
	{
		Codec codec = this.factory.createCodec(null);
		
		Assert.assertEquals("password", codec.decode("B:cGFzc3dvcmQ="));
	}
	
	@Test
	public void test() throws SQLException
	{
		Codec codec = this.factory.createCodec("cluster");
		
		Assert.assertEquals("password", codec.decode("C:wzAkF0hlYUeGhfzRQIxYAQ=="));
	}
}
