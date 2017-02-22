package com.imn.hdfs.api.servic;


import java.util.List;
	
import org.apache.log4j.Logger;

import com.imn.hdfs.api.form.APIResponseForm;
import com.imn.hdfs.api.utils.HDFSUtils;

public class APIController {

		
	
	private static final Logger HDFS_LOGS = Logger	.getLogger("HDFSLOGGER");

	
	private static final Logger HDFS_EDRS = Logger	.getLogger("HDFSEDRS");

	
		
	public List<APIResponseForm> createAPI() {
		
		System.out.println("Inside the CreateAPI");
		List<APIResponseForm> listOfFiles=null;
		
		listOfFiles=HDFSUtils.connect();
		
		
		HDFS_LOGS.info("##### Enter into the HDFS_LOGS ");
		
		
		return listOfFiles;
		
	
	}
	
	
	
}
