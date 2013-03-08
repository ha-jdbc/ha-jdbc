package net.sf.hajdbc.io;

import net.sf.hajdbc.io.simple.SimpleInputSinkStrategy;

public class SimpleInputSinkStrategyTest extends InputSinkStrategyTest<byte[]>
{
	public SimpleInputSinkStrategyTest()
	{
		super(new SimpleInputSinkStrategy());
	}
}
