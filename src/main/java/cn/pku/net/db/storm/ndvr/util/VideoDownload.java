/**
 * @Package cn.pku.net.db.storm.ndvr.util
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.util;

import cn.pku.net.db.storm.ndvr.common.Const;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Description: Download video and keyframes of the CC_WEB_VIDEO dadaset
 * @author jeremyjiang
 * Created at 2016/5/12 17:23
 */
public class VideoDownload {
    private static Logger logger        = Logger.getLogger(VideoDownload.class);
    private String        videoListPath = Const.CC_WEB_VIDEO.VIDEO_INFO_PATH;
    private String        videoSaveDir  = Const.CC_WEB_VIDEO.VIDEO_PATH_PREFIX;

    public void downloadVideo() {
        File           videoListFile = new File(videoListPath);
        BufferedReader reader        = null;

        try {
            reader = new BufferedReader(new FileReader(videoListFile));

            String line = null;

            while ((line = reader.readLine()) != null) {
                String[] infoArr = line.split("\t");

                // if (Integer.parseInt(infoArr[0]) <= 10578)
                // continue;
                String   videoFileUrl  = "http://vireo.cs.cityu.edu.hk/webvideo/videos/" + infoArr[1] + "/"
                                         + infoArr[3];
                String   videoSaveFile = videoSaveDir + infoArr[1] + "\\" + infoArr[3];
                Runnable r             = new DownloadThread(videoFileUrl, videoSaveFile);

                MyThreadPool.getInstance().getPool().submit(r);
            }
        } catch (FileNotFoundException e) {
            logger.error("VideoList file not found. ", e);
        } catch (IOException e) {
            logger.error("IOException when read VideoList file. ", e);
        }
    }

    public static void main(String[] args) {
        VideoDownload vdl = new VideoDownload();

        vdl.downloadVideo();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
