package com.casicloud.aop.kafka.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
@Component
public class HDFSClinet{
	@Autowired
	@Qualifier("hadoopFS")
	private FileSystem hadoopFS;
	
	/** 
     * 判断路径是否存在 
     * @param path 
     * @return 
     * @throws IOException 
     */  
    public  boolean exits(String path) throws IOException {  
        return hadoopFS.exists(new Path(path));  
    }  
  
    /** 
     * 创建文件 
     * @param filePath 
     * @param contents 
     * @throws IOException 
     */  
    public  void createFile(String filePath, byte[] contents)  
            throws IOException {  
        Path path = new Path(filePath);  
        FSDataOutputStream outputStream = hadoopFS.create(path);  
        outputStream.write(contents);  
        outputStream.close();  
          
    }  
    /** 
     * 追加文件 
     * @param filePath 
     * @param contents 
     * @throws IOException 
     */  
    public  void appendToFile(String filePath, String contents)  
    		throws IOException {  
    	Path path = new Path(filePath);  
    	if (!exits(filePath)) {
			createFile(filePath, contents);
		}else{
			FSDataOutputStream outputStream = hadoopFS.append(path, 1024*64);  
			outputStream.write(contents.getBytes());  
			outputStream.close();  
		}
    	
    }  
  
    /** 
     * 创建文件 
     *  
     * @param filePath 
     * @param fileContent 
     * @throws IOException 
     */  
    public  void createFile(String filePath, String fileContent)  
            throws IOException {  
        createFile(filePath, fileContent.getBytes());  
    }  
  
    /** 
     * 上传本地文件
     * @param localFilePath 
     * @param remoteFilePath 
     * @throws IOException 
     */  
    public  void copyFromLocalFile(String localFilePath,  
            String remoteFilePath) throws IOException {  
        Path localPath = new Path(localFilePath);  
        Path remotePath = new Path(remoteFilePath);  
        hadoopFS.copyFromLocalFile(false, true, localPath, remotePath);  
          
    }  
  
    /** 
     * 删除目录或文件 
     *  
     * @param conf 
     * @param remoteFilePath 
     * @param recursive 
     * @return 
     * @throws IOException 
     */  
    public  boolean deleteFile(String remoteFilePath, boolean recursive)  
            throws IOException {  
        boolean result = hadoopFS.delete(new Path(remoteFilePath), recursive);  
          
        return result;  
    }  
  
    /** 
     * 删除目录或文件(如果有子目录,则级联删除) 
     *  
     * @param conf 
     * @param remoteFilePath 
     * @return 
     * @throws IOException 
     */  
    public  boolean deleteFile(String remoteFilePath) throws IOException {  
        return deleteFile(remoteFilePath, true);  
    }  
  
    /** 
     * 文件重命名 
     *  
     * @param oldFileName 
     * @param newFileName 
     * @return 
     * @throws IOException 
     */  
    public  boolean renameFile(String oldFileName, String newFileName)  
            throws IOException {  
        Path oldPath = new Path(oldFileName);  
        Path newPath = new Path(newFileName);  
        boolean result = hadoopFS.rename(oldPath, newPath);  
          
        return result;  
    }  
  
    /** 
     * 创建目录 
     * @param dirName 
     * @return 
     * @throws IOException 
     */  
    public  boolean createDirectory(String dirName) throws IOException {  
        Path dir = new Path(dirName);  
        boolean result = false;  
        if (!hadoopFS.exists(dir)) {  
            result = hadoopFS.mkdirs(dir);  
        }  
          
        return result;  
    }  
  
    /** 
     * 列出指定路径下的所有文件(不包含目录) 
     *  
     * @param basePath 
     * @param recursive 
     */  
    public  RemoteIterator<LocatedFileStatus> listFiles(String basePath,  
            boolean recursive) throws IOException {  
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = hadoopFS  
                .listFiles(new Path(basePath), recursive);  
  
        return fileStatusRemoteIterator;  
    }  
  
    /** 
     * 列出指定路径下的文件（非递归） 
     *  
     * @param basePath 
     * @return 
     * @throws IOException 
     */  
    public  RemoteIterator<LocatedFileStatus> listFiles(String basePath)  
            throws IOException {  
        RemoteIterator<LocatedFileStatus> remoteIterator = hadoopFS.listFiles(  
                new Path(basePath), false);  
          
        return remoteIterator;  
    }  
  
    /** 
     * 列出指定目录下的文件\子目录信息（非递归） 
     *  
     * @param dirPath 
     * @return 
     * @throws IOException 
     */  
    public  FileStatus[] listStatus(String dirPath) throws IOException {  
        FileStatus[] fileStatuses = hadoopFS.listStatus(new Path(dirPath));  
          
        return fileStatuses;  
    }  
  
    /** 
     * 读取文件内容 
     * 
     * @param filePath 
     * @return 
     * @throws IOException 
     */  
    public  byte[] readFile(String filePath) throws IOException {  
        byte[] fileContent = null;  
        Path path = new Path(filePath);  
        if (hadoopFS.exists(path)) {  
            InputStream inputStream = null;  
            ByteArrayOutputStream outputStream = null;  
            try {  
                inputStream = hadoopFS.open(path);  
                outputStream = new ByteArrayOutputStream(  
                        inputStream.available());  
                IOUtils.copyBytes(inputStream, outputStream, 2048);  
                fileContent = outputStream.toByteArray();  
            } finally {  
                IOUtils.closeStream(inputStream);  
                IOUtils.closeStream(outputStream);  
                  
            }  
        }  
        return fileContent;  
    }  
    public void download(String remote, String local) throws IOException {  
        Path path = new Path(remote);  
        hadoopFS.copyToLocalFile(path, new Path(local));  
        System.out.println("download: from" + remote + " to " + local);  
          
    }  
}
