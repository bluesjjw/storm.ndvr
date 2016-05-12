/**
 * @Package cn.pku.net.db.storm.ndvr.util
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.log4j.Logger;

/**
 * Description: file download thread
 * @author jeremyjiang
 * Created at 2016/5/12 16:56
 */
public class DownloadThread implements Runnable {
    private static Logger logger = Logger.getLogger(DownloadThread.class);
    private final String  fileUrl;
    private final String  savePath;

    /**
     * Instantiates a new Download thread.
     *
     * @param fileUrl  the file url
     * @param saveFile the save file
     */
    public DownloadThread(String fileUrl, String saveFile) {
        this.fileUrl  = fileUrl;
        this.savePath = saveFile;
    }

    /**
     * @see java.lang.Runnable#run()
     */
    public void run() {
        URL url = null;

        try {
            url = new URL(fileUrl);

            URLConnection    conn      = url.openConnection();
            InputStream      inStream  = conn.getInputStream();
            FileOutputStream outStream = new FileOutputStream(savePath);
            byte[]           buffer    = new byte[1024];
            int              byteCount = 0;
            int              byteSum   = 0;

            while ((byteCount = inStream.read(buffer)) != -1) {
                byteSum += byteCount;
                outStream.write(buffer, 0, byteCount);
            }

            logger.info("Download file success. FileName: " + savePath + ". FileSize: " + byteSum + " Byte.");
        } catch (MalformedURLException e) {
            logger.error("MalformedURL. ", e);
        } catch (IOException e) {
            logger.error("IOException when download file. ", e);
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
