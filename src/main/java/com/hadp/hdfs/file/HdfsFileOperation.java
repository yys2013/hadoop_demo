package com.hadp.hdfs.file;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class HdfsFileOperation {

    Logger logger = Logger.getLogger(HdfsFileOperation.class);
    
    Configuration conf = null;
    FileSystem fs = null;

    @Before
    public void initConf() throws Exception {
        conf = new Configuration();
        String uri = "hdfs://10.10.22.176:9000";
        fs = FileSystem.get(URI.create(uri), conf);
    }

    @Before
    public void afterOper() throws Exception {
        if (fs != null) {
            fs.close();
        }
    }

    public void listFiles(String path) throws Exception {
        FileStatus[] listStatus = fs.listStatus(new Path(path));
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isDirectory()) {
                logger.info(fileStatus.getPath());
                listFiles(fileStatus.getPath().toString());
            } else {
                //System.out.println(fileStatus.getPath());
                logger.info(fileStatus.getPath());
            }
        }
    }

    /**
     * 列出所有文件
     * @throws Exception
     */
    @Test
    public void testListFiles() throws Exception {
        String path = "/";
        listFiles(path);
    }

    /**
     * 下载文件
     * @throws Exception
     */
    @Test
    public void downloadFile() throws Exception {
        Path srcPath = new Path("/user/aifjs/data/input/纯音乐-Last Tango In Paris巴黎最后的探戈.mp3");
        Path dstPath = new Path("E:/");
        fs.copyToLocalFile(srcPath, dstPath);
    }

    /**
     * 上传文件
     * @throws Exception
     */
    @Test
    public void uploadFile() throws Exception {
        Path localPath = new Path("F:\\111\\纯音乐-Last Tango In Paris巴黎最后的探戈.mp3");
        Path hdfsPath = new Path("/user/aifjs/data/input/");
        fs.copyFromLocalFile(localPath, hdfsPath);
    }
    
    /**
     * 创建文件夹
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        Path path = new Path("/user/aifjs/data/music");
        fs.mkdirs(path);
    }
    
    /**
     * mv
     * 移动文件
     * @throws Exception
     */
    @Test
    public void mvFile() throws Exception {
        Path srcPath = new Path("/user/aifjs/data/input/纯音乐-Last Tango In Paris巴黎最后的探戈.mp3");
        FSDataInputStream in = fs.open(srcPath); 
        Path path = new Path("/user/aifjs/data/music/纯音乐-Last Tango In Paris巴黎最后的探戈.mp3");
        FSDataOutputStream out = fs.create(path);
        byte[] b = new byte[1024]; 
        int len = 0;
        while ((len = in.read(b)) > 0) {
            out.write(b, 0, len); 
        }
        if(fs.exists(srcPath)) {
            fs.delete(srcPath, true);
        }
        in.close();
        out.close();
    }
}
