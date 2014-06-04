package com.hortonworks.yarnapp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AppClient {
	private static final Logger LOG = LoggerFactory.getLogger(AppClient.class);
  
	private static final String APP_NAME = "AppClient";
	private YarnConfiguration conf;
	private YarnClient yarnClient;
	private String appJar = "yarnapp.jar";
	private ApplicationId appId;
	private FileSystem fs;
	private String inputPath;
	private String search;
	private String outputFolder;
	
	// constructor
	public AppClient(String[] args) throws IOException{
    
		// Create configuration (from Hadoop config files in classpath)
		this.conf = new YarnConfiguration();
		
		// Client implements ApplicationClient interface
		this.yarnClient = YarnClient.createYarnClient();
		this.yarnClient.init(conf);
		this.yarnClient.start();

		// Initialize filesystem
		this.fs = FileSystem.get(conf);
		
		// Get command line arguements
		// 0 = input file
		// 1 = input folder
		// 2= output folder
		
		// Input path
		// TODO: Error checking on these
		this.inputPath = args[0];
		this.search = args[1];
		this.outputFolder = args[2];
		
	}
	
	/**
	 * Run method
	 * 
	 * @return whether app ran I think
	 * @throws YarnException
	 * @throws IOException
	 */
	public boolean run() throws YarnException, IOException{
    
		// Create application
		GetNewApplicationResponse appResponse = 
		    this.yarnClient.createApplication().getNewApplicationResponse();
		
		// Get response
		this.appId = appResponse.getApplicationId();
		LOG.info("Application ID = {}", appId);
		
		// View cluster info/metrics and log
		int maxClusterMemory = 
		    appResponse.getMaximumResourceCapability().getMemory();
		int maxVCores = 
		    appResponse.getMaximumResourceCapability().getVirtualCores();
		LOG.info("Max memory = {}\tMax v-cores = {}",
		    maxClusterMemory, maxVCores);
		
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    
		int nodeManagers = clusterMetrics.getNumNodeManagers();
		LOG.info("Number of node managers = {}", nodeManagers);
		
		// Log node info
		List<NodeReport> nodeReports = yarnClient
				.getNodeReports(NodeState.RUNNING);
		for(NodeReport report : nodeReports){
			LOG.info("Node ID = {}, Address = {}, containers = {}",
					report.getNodeId(), report.getHttpAddress(),
					report.getNumContainers());
		}
		
		// Log queue info
		List<QueueInfo> queueReports = yarnClient.getAllQueues();
		for(QueueInfo queue : queueReports){
			LOG.info("Queue Name = {}, Capacity = {}, Max Capacity = {}",
					queue.getQueueName(), queue.getCapacity(), 
					queue.getMaximumCapacity());
		}
		
		
		// Add the jar to HDFS 
		Path src = new Path(this.appJar);
		String pathSuffix = APP_NAME + "/" + appId.getId() + "/app.jar";
		Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
		fs.copyFromLocalFile(false,  true, src, dst);
		FileStatus destStatus = fs.getFileStatus(dst);
		
		// Create jar as local resource
		LocalResource jarResource = Records.newRecord(LocalResource.class);
		jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dst));
		jarResource.setSize(destStatus.getBlockSize());
		jarResource.setTimestamp(destStatus.getModificationTime());
		jarResource.setType(LocalResourceType.FILE);
		jarResource.setVisibility(LocalResourceVisibility.APPLICATION);
		
		// Local Resource hash map
		Map<String, LocalResource> localResources = 
				new HashMap<String, LocalResource>();
		localResources.put("app.jar", jarResource);
		
		// Building environment hash map
		Map<String, String> env = new HashMap<String, String>();
		env.put("AMJARTIMESTAMP", Long.toString(jarResource.getTimestamp()));
		env.put("AMJARLEN", Long.toString(jarResource.getSize()));
		
		String appJarDst = dst.toUri().toString();
		env.put("AMJAR", appJarDst);
		LOG.info("AMJAR environment variable set to {}", appJarDst);
		
		// Configure the launch environment
		StringBuilder classPathEnv = new StringBuilder();
		classPathEnv.append(File.pathSeparatorChar).append("./app.jar");
		for(String c : conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)){
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(c.trim());
		}
		classPathEnv.append(File.pathSeparatorChar);
		classPathEnv.append(Environment.CLASSPATH.$());
		env.put("CLASSPATH", classPathEnv.toString());
		
		// Configure the application context
		YarnClientApplication app = this.yarnClient.createApplication();
		ApplicationSubmissionContext appContext = 
				app.getApplicationSubmissionContext();
		appContext.setApplicationName(APP_NAME);
	
		// Configure Launch Context
		ContainerLaunchContext amContainer = 
				Records.newRecord(ContainerLaunchContext.class);
		amContainer.setLocalResources(localResources);
		amContainer.setEnvironment(env);
		
		// Configure the command line for the application manager
		Vector<CharSequence> vargs = new Vector<>(30);
		vargs.add(Environment.JAVA_HOME.$()+"/bin/java");
		vargs.add("com.hortonworks.yarnapp.ApplicationMaster");
		vargs.add("1><LOG_DIR>/AppClient.stdout");
		vargs.add("2><LOG_DIR>/AppClient.stderr");
		vargs.add(this.inputPath);
		vargs.add(this.search);
		vargs.add(this.outputFolder);
		StringBuilder command = new StringBuilder();
		for(CharSequence c : vargs){
			command.append(c);
			command.append(" ");
		}
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		amContainer.setCommands(commands);
		
		LOG.info("Command to execute ApplicationMaster = {}",command);
		
		// Request Memory (supposed to be 1GB, I'm only going to use 512M)
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(512);
		appContext.setResource(capability);
		
		// Submit container launch context to resource manager
		appContext.setAMContainerSpec(amContainer);
		
		// Submit the application
		yarnClient.submitApplication(appContext);
		
		
		return true;
	}
  
	@SuppressWarnings("unused")	// TODO - Remove this
	public static void main(String[] args){
    
		AppClient appClient;
		
		try {
			appClient = new AppClient(args);
			boolean result = appClient.run();
		
		} catch (YarnException e) {
			LOG.error("SUCESS:Yarn Error was sucessfully caught");
			LOG.error(e.getMessage());
			e.printStackTrace();
			
		} catch (IOException e) {
			LOG.error("SUCESS: IO Error was sucessfully caught");
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
    
	}
  
}
