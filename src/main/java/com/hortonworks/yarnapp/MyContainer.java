package com.hortonworks.yarnapp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class MyContainer {

	private static final Logger LOG = 
			LoggerFactory.getLogger(MyContainer.class);

	private String hostname;
	private YarnConfiguration conf;
	private FileSystem fileSystem;
	private Path inputFile;
	private Path outputFolder;
	private String search;
	private long start;
	private int length;
	
	
	public MyContainer(String[] args) throws IOException {
		
		this.hostname = NetUtils.getHostname();
		this.conf = new YarnConfiguration();
		this.fileSystem = FileSystem.get(conf);
		
		// File to process
		// usage: file in_folder out_folder start size
		this.inputFile = new Path(args[0]);
		this.search = args[1];
		this.outputFolder = new Path(args[2]);
		this.start = Long.parseLong(args[3]);
		this.length = Integer.parseInt(args[4]);
		
		
		
	}
	
	public void run() throws IOException{
		
		List<String> lines = new ArrayList<String>();
		
		LOG.info("Running the container on{}", this.hostname);
		
		FSDataInputStream fsdis = fileSystem.open(this.inputFile);
		fsdis.seek(this.start);
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(fsdis));
		LOG.info("Reading from {} to {} from {}", start, start + length,
				this.inputFile.toString());
		
		String current = "";
		long bytesRead = 0;
		while(bytesRead < this.length &&
				(current = reader.readLine()) != null) {
			bytesRead = current.getBytes().length;
			if(current.contains(search)){
				LOG.info("Found CLINTON: {}", current);
				lines.add(current);
			}
		}
		
		// Store output on hdp
		Path outputFile = new Path(this.outputFolder.toString() 
				+ "/result_" + start);
		FSDataOutputStream fsout = this.fileSystem.create(outputFile);
		BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(fsout));
		for(String line : lines){
			writer.write(current + "\n");
		}
		
	}
	
	public static void main(String[] args) {
		
		LOG.info("Starting container on {}", NetUtils.getHostname());
		
		try {
			MyContainer container = new MyContainer(args);
			container.run();
			
		} catch(IOException e) {
			LOG.error("SUCESS: IOException caught sucessfully");
			LOG.error(e.getMessage());
			e.printStackTrace();
			
		}
		
		LOG.info("Container is ending ...");
		
	}
	
}
