package net.sf.hajdbc.io;

import java.io.File;

import net.sf.hajdbc.io.file.FileInputSinkStrategy;

public class FileInputSinkStrategyTest extends InputSinkStrategyTest<File>
{
	public FileInputSinkStrategyTest()
	{
		super(new FileInputSinkStrategy());
	}
}
