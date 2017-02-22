package com.imn.hdfs.api.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.imn.hdfs.api.form.APIResponseForm;

public class HDFSUtils {

	
	public static List<APIResponseForm> connect(){
		List<APIResponseForm> apiRes=new ArrayList<APIResponseForm>();
		try{
			System.setProperty("hadoop.home.dir", "/");

			Configuration conf = new Configuration();
			
			/*conf.addResource(new Path("/home/hadoop/spark/hadoop-2.6.0/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/home/hadoop/spark/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));
			conf.addResource(new Path("/home/hadoop/spark/hadoop-2.6.0/etc/hadoop/mapred-site.xml"));*/
			
			//conf.set("fs.defaultFS", "hdfs://localhost:8020");
			FileSystem fileSystem = FileSystem.get(conf);
			System.out.println(fileSystem.getName());
			
			
			
			int count =0;
			boolean recursive = false;
			
			FileStatus[] listStatus = fileSystem.listStatus(new Path("hdfs://localhost:8020/output/"));
			
				
				for (FileStatus fileStatus : listStatus) {
				
				Path path= fileStatus.getPath();
				
				StringBuffer sb=new StringBuffer();
				if(path.getName()!=null){
					APIResponseForm res=new APIResponseForm();
					res.setFilesName(path.getName());
				//System.out.println("File Name is============================>>****************>>> "+path.getName());
				
				FSDataInputStream in = fileSystem.open(path);
				
				count++;
				//String filename = file.substring(file.lastIndexOf('/') + 1,file.length());

			
				
			     BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                 String line;
                 line=br.readLine();
                 while (line != null){
                	 sb.append(line);
                	 sb.append("\n");
                
                         line=br.readLine();
                 }
                 res.setFileInfo(sb.toString());
                
                 apiRes.add(res);
				}  else{
					
					System.out.println("===============********************** ==========================="+path);
				}
                 
                 
			}
			
			System.out.println("=========================*********** End Here *********************==========================");
			System.out.println("==================================================================COUNT IS "+count);
			return apiRes;
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	/*public static List<APIResponseForm>   path(){
		
		
		List<APIResponseForm> apiRes=new ArrayList<APIResponseForm>();
		try{
			System.setProperty("hadoop.home.dir", "/");

			Configuration conf = new Configuration();
			
			conf.addResource(new Path("/home/hadoop/spark/hadoop-2.6.0/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/home/hadoop/spark/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));
			conf.addResource(new Path("/home/hadoop/spark/hadoop-2.6.0/etc/hadoop/mapred-site.xml"));
			
			conf.set("fs.defaultFS", "hdfs://localhost:8020");
			FileSystem fileSystem = FileSystem.get(conf);
			System.out.println(fileSystem.getName());
			
			
			
			int count =0;
			boolean recursive = false;
			RemoteIterator<LocatedFileStatus> ri = fileSystem.listFiles(new Path("hdfs://localhost:8020/output/"), recursive);
			
			
			System.out.println("==================================================================");
			System.out.println("================************* Start Here **********============================");
			while (ri.hasNext()){

				LocatedFileStatus lfs=(LocatedFileStatus)ri.next();
				
				Path path= lfs.getPath();
				
				StringBuffer sb=new StringBuffer();
				if(path.getName()!=null){
					APIResponseForm res=new APIResponseForm();
					res.setFilesName(path.getName());
				//System.out.println("File Name is============================>>****************>>> "+path.getName());
				
				FSDataInputStream in = fileSystem.open(path);
				
				count++;
				//String filename = file.substring(file.lastIndexOf('/') + 1,file.length());

			
				
			     BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                 String line;
                 line=br.readLine();
                 while (line != null){
                	 sb.append(line);
                	 sb.append("\n");
                
                         line=br.readLine();
                 }
                 res.setFileInfo(sb.toString());
                
                 apiRes.add(res);
				}  else{
					
					System.out.println("===============********************** ==========================="+path);
				}
                 
                 
			}
			
			System.out.println("=========================*********** End Here *********************==========================");
			System.out.println("==================================================================COUNT IS "+count);
			return apiRes;
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
*/	
	public static void main(String[] args) {
		List<APIResponseForm> list=connect();
		for(APIResponseForm res:list){
			System.out.println(res.getFileInfo()+"  "+res.getFilesName());
		}
	}
}
