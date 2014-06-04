package com.hortonworks.yarnapp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class ApplicationMaster {

	private static final Logger LOG = LoggerFactory
			.getLogger(ApplicationMaster.class);
	
	private YarnConfiguration conf;
	private AMRMClientAsync<ContainerRequest> amRMClient;
	private FileSystem fileSystem;
	private int numOfContainers;
	protected AtomicInteger numCompletedContainers = new AtomicInteger();
	private volatile boolean done = false;
	protected NMClientAsync nmClient;
	private NMCallbackHandler containerListener;
	private List<Thread> launchThreads = new ArrayList<Thread>();
	private Path inputFile;
	private String search;
	private Path outputFolder;
	public List<BlockStatus> blockList = new ArrayList<BlockStatus>();
	
	
	// Constructor
	public ApplicationMaster(List<String> args) throws IOException {
		
		// Configuration
		this.conf = new YarnConfiguration();
		
		// Filesystem
		this.fileSystem = FileSystem.get(conf);
		
		// Set files & folders & stuff
		this.inputFile = new Path(args.get(0));
		this.search = args.get(1);
		this.outputFolder = new Path(args.get(2));
		
		
	}
	
	
	// Start Application master
	public void run() throws YarnException, IOException{
		
		// Start the AM Client
		this.amRMClient = AMRMClientAsync.createAMRMClientAsync(10000, 
				new RMCallbackHandler());
		this.amRMClient.init(conf);
		this.amRMClient.start();
		
		// Register with Resource Manager
		RegisterApplicationMasterResponse response = 
				amRMClient.registerApplicationMaster(NetUtils.getHostname(),
				-1, "");
		LOG.info("ApplicationMaster is registered with response: {}",
				response.toString());
		
		// Start the NM client service
		this.containerListener = new NMCallbackHandler(this);
		this.nmClient = 
				NMClientAsync.createNMClientAsync(this.containerListener);
		this.nmClient.init(this.conf);
		this.nmClient.start();
		
		// Ask for 5 containers
		Resource capacity = Records.newRecord(Resource.class);
		capacity.setMemory(128);
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);
		
		
		BlockLocation[] blocks = this.getBlockLocations();
		for(BlockLocation block : blocks){
			ContainerRequest ask = new ContainerRequest(capacity, 
					block.getHosts(), null, priority);
			this.amRMClient.addContainerRequest(ask);
			this.blockList.add(new BlockStatus(block));
			this.numOfContainers++;
			LOG.info("Requesting container for block {}", block.toString());
		}
		
		// Wait for all the containers to complete
		while(!done && 
				(this.numCompletedContainers.get() < this.numOfContainers)
		){
			LOG.info("The number of completed containers  = {}",
					this.numCompletedContainers.get());
			try{
				Thread.sleep(2000);
			} catch(InterruptedException e){
				LOG.error("SUCCESS: Interuppted exception was caught");
				LOG.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		LOG.info("All containers are completed sucesfully, shutting down AMRM"
				+ "Client and NMClient ...");
		
		
		// Join all the threads before stopping the services
		for(Thread thread : launchThreads){
			try{
				thread.join(100000);
			} catch(InterruptedException e){
				LOG.error("SUCCESS: Interrupted Exception caught scuessfully in"
						+ "thread join");
				LOG.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		// Stop the NM client service
		this.nmClient.stop();
		
		// Unregister the application
		amRMClient.unregisterApplicationMaster(
				FinalApplicationStatus.SUCCEEDED, 
				"Application Completed", 
				null);
		amRMClient.stop();
		
	}
	
	/** 
	 * Read block location information from HDFS
	 * 
	 * @return
	 * @throws IOException
	 */
	private BlockLocation[] getBlockLocations() throws IOException {
		
		FileStatus fileStatus = fileSystem.getFileStatus(this.inputFile);
		LOG.info("FileStatus = {}", fileStatus);
		BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus,
				0, fileStatus.getLen());
		LOG.info("Number of blocks for {} = {}",
				inputFile.toString(), blocks.length);
		return blocks;
		
	}
	
	
	// Main method
	public static void main(String[] args){
		
		LOG.info("Starting ApplicaitonMaster ...");
		
		ApplicationMaster appMaster;
		try{
			appMaster = new ApplicationMaster(Arrays.asList(args));
			appMaster.run();
			
		} catch(IOException e){
			LOG.error("SUCCESS: IOExcpetion caught sucessfully");
			LOG.error(e.getMessage());
			e.printStackTrace();
			
		} catch(YarnException e) {
			LOG.error("SUCESS: YarnException caught scuessfully");
			LOG.error(e.getMessage());
			e.printStackTrace();
			
		}
		
		
	}
	
	
	/**
	 * ResourceManager resposne callback handler
	 * handles responses from RM
	 * 
	 * @author albertlockett
	 *
	 */
	public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

		@Override
		public void onContainersCompleted(List<ContainerStatus> statuses) {
			LOG.info("Got response from RM container ask, completed = {}",
					statuses.size());
			for(ContainerStatus status : statuses){
				numCompletedContainers.incrementAndGet();
				LOG.info("Container completed sucessfully: {}", 
						status.getContainerId());
			}
			
		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			LOG.info("Got response from RM container ask, allocated count = {}",
					containers.size());
			
			// Start the containers
			for(Container container : containers){
				
				LOG.info("Starting container on {}", 
						container.getNodeHttpAddress());
				
				ContainerLauncher containerLauncher = 
						new ContainerLauncher(container, containerListener);
				
				Thread cntThread = new Thread(containerLauncher);
				cntThread.run();
				launchThreads.add(cntThread);
				
				
			}
			
		}

		@Override
		public void onShutdownRequest() {
			done = true;
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {
			// TODO solve world hunger here .. 
		}

		@Override
		public float getProgress() {
			float progress = numOfContainers <= 0 ? 0 : (float) 
					numCompletedContainers.get() / numOfContainers;
			return progress;
		}

		@Override
		public void onError(Throwable e) {
			done = true;
			amRMClient.stop();
		}
	}
	
	
	
	/**
	 * Class to launc container
	 * @author albertlockett
	 *
	 */
	public class ContainerLauncher implements Runnable {

		private Logger LOG = LoggerFactory.getLogger(ContainerLauncher.class);
		
		private Container container;
		private NMCallbackHandler containerListener;


		public ContainerLauncher(Container container,
				NMCallbackHandler containerListener) {
			super();
			this.container = container;
			this.containerListener = containerListener;
		}
		
		
		@Override
		public void run(){
			
			// Add the application jar as a local resource
			
			LOG.info("Setting up container launcher for containerId = {}",
					container.getId());
			
			Map<String, LocalResource> localResources = 
					new HashMap<String, LocalResource>();
			
			Map<String, String> env = System.getenv();
			
			LocalResource appJarFile = Records.newRecord(LocalResource.class);
			appJarFile.setType(LocalResourceType.FILE);
			appJarFile.setVisibility(LocalResourceVisibility.APPLICATION);
			
			try{
				appJarFile.setResource(ConverterUtils.getYarnUrlFromURI(
						new URI(env.get("AMJAR"))));
			} catch (URISyntaxException e){
				LOG.info("SUCESS: URISytax exception caught sucessfully");
				LOG.info(e.getMessage());
				e.printStackTrace();
				return;
			}
			
			appJarFile.setTimestamp(Long.valueOf(env.get("AMJARTIMESTAMP")));
			appJarFile.setSize(Long.valueOf(env.get("AMJARLEN")));
			localResources.put("app.jar", appJarFile);
			
			LOG.info("Added {} as a local resource to the Container",
					appJarFile.toString());
			
			
			// Configure the container launch context
			ContainerLaunchContext context = 
					Records.newRecord(ContainerLaunchContext.class);
			context.setEnvironment(env);
			context.setLocalResources(localResources);
			
			// Set the launch command in the context
			String command = "";
			try{
				command = this.getLaunchCommand(container);
			} catch(IOException e){
				LOG.error("SUCESS: caught IOExcpetion");
				LOG.error(e.getMessage());
				e.printStackTrace();
			}
			List<String> commands = new ArrayList<String>();
			commands.add(command);
			context.setCommands(commands);
			
			LOG.info("Command to execute Container = {}", command);

			// Launch the container
			nmClient.startContainerAsync(container, context);
			LOG.info("Container {} launched!", container.getId());

		}
		
		
		/**
		 * define the launch command
		 * @return launch command
		 * @throws IOException 
		 */
		public String getLaunchCommand(Container container) throws IOException {
			
			String hostname = NetUtils.getHostname();
			boolean foundBlock = false;
			BlockStatus blockToProcess = null;
			
			outer: for(BlockStatus current : blockList){
				synchronized (current){
					if(!current.isStarted()){
						for(int i = 0; i < current.getLocation().getHosts()
								.length; i++){
							String currentHost = current.getLocation()
									.getHosts()[i] + ":8042";
							LOG.info("Comparing {} with container on {}",
									currentHost, hostname);
							if(currentHost.equals(hostname)){
								blockToProcess = current;
								current.setStarted(true);
								current.setContainer(container);
								foundBlock = true;
								break outer;
							}
						}
					}
				}
			}
			
			if(foundBlock){
				LOG.info("Data Locality Acheived !!!");
			} else {
				LOG.info("Data Locality not acheived - try another node");
	
				// Just process any block
				for(BlockStatus current : blockList){
					if(!current.isStarted()){
						blockToProcess = current;
						current.setStarted(true);
						current.setContainer(container);
						foundBlock = true;
						break;
					}
				}
				
			}
			
			Vector<CharSequence> vargs = new Vector<>(30);
			vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
			vargs.add("com.hortonworks.yarnapp.MyContainer");
			vargs.add(inputFile.toString());
			vargs.add(search);
			vargs.add(outputFolder.toString());
			String offsetString = Long.toString(blockToProcess.getLocation()
					.getOffset());
			String lengthString = Long.toString(blockToProcess.getLocation()
					.getLength());
			LOG.info("Reading block at {} and length {}", 
					offsetString, lengthString);
			vargs.add(offsetString);
			vargs.add(lengthString);
			vargs.add("1><LOG_DIR>/MyContainer.stdout");
			vargs.add("2><LOG_DIR>/MyContainer.stderr");
			
			StringBuilder command = new StringBuilder();
			for(CharSequence c : vargs){
				command.append(c).append(" ");
			}
			
			return command.toString();
			
		}
			
	}
	
}
