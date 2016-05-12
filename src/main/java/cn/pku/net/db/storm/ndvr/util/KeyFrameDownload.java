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
 * Description: Download keyframes of the CC_WEB_VIDEO dataset
 *
 * @author jeremyjiang
 * Created at 2016/5/12 17:13
 */
public class KeyFrameDownload {
    private static Logger logger           = Logger.getLogger(KeyFrameDownload.class);
    private String        keyframeInfoPath = Const.CC_WEB_VIDEO.KEYFRAME_INFO_PATH;
    private String        keyframeDir      = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX;

    /**
     * Download keyframes.
     */
    public void downloadKeyFrame() {
        File           keyframeListFile = new File(keyframeInfoPath);
        BufferedReader reader           = null;

        try {
            reader = new BufferedReader(new FileReader(keyframeListFile));

            String line = null;

            while ((line = reader.readLine()) != null) {
                String[] infoArr = line.split("\t");

                if (Integer.parseInt(infoArr[2]) / 100 < 119) {
                    continue;
                }

                String kfUrl = "http://vireo.cs.cityu.edu.hk/webvideo/Keyframes/" + Integer.parseInt(infoArr[2]) / 100
                               + "/" + infoArr[1];
                String kfSaveDirStr = keyframeDir + Integer.parseInt(infoArr[2]) / 100 + "\\";
                File   kfSaveDir    = new File(kfSaveDirStr);

                if (!kfSaveDir.exists()) {
                    kfSaveDir.mkdirs();
                    logger.info("创建文件夹: " + kfSaveDirStr);
                }

                String   kfSaveFile = kfSaveDirStr + infoArr[1];
                Runnable r          = new DownloadThread(kfUrl, kfSaveFile);

                MyThreadPool.getInstance().getPool().submit(r);
            }
        } catch (FileNotFoundException e) {
            logger.error("Shot_Info file not found. ", e);
        } catch (IOException e) {
            logger.error("IOException when read Shot_Info file. ", e);
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        KeyFrameDownload kfd = new KeyFrameDownload();

        kfd.downloadKeyFrame();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
