/**
 * @Package cn.pku.net.db.storm.ndvr.util
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.util;

import java.awt.Color;
import java.awt.image.BufferedImage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.List;

import javax.imageio.ImageIO;

import org.apache.log4j.Logger;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;

/**
 * Description: Generate global visual signature
 *
 * @author jeremyjiang
 * Created at 2016/5/12 16:58
 */
public class GlobalSigGenerator {
    private static final Logger logger = Logger.getLogger(GlobalSigGenerator.class);

    /**
     * Generate hsv sigature with a list of keyframes.
     *
     * @param keyframeList the keyframe list
     * @return the hsv signature entity
     */
    public static HSVSigEntity generate(List<KeyFrameEntity> keyframeList) {
        float[] hsvSignature = new float[24];    // 视频的HSV标签,前18为H分类,3为S分类,3为V分类
        int     count        = 0;                // 记录有效关键帧数量

        for (KeyFrameEntity keyframeEnt : keyframeList) {
            String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                                  + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "/"
                                  + keyframeEnt.getKeyFrameName();
            InputStream input = null;

            try {
                input = new FileInputStream(new File(keyframeFile));

                BufferedImage image    = ImageIO.read(input);
                float[]       imageHsv = new float[24];    // 一个关键帧的HSV标签

                for (int posx = 0; posx < image.getWidth(); posx++) {
                    for (int posy = 0; posy < image.getHeight(); posy++) {
                        Color   rgbColor = new Color(image.getRGB(posx, posy));
                        float[] pixelHsv = new float[3];

                        Color.RGBtoHSB(rgbColor.getRed(), rgbColor.getGreen(), rgbColor.getBlue(), pixelHsv);
                        hsvQuantization(pixelHsv, imageHsv);

                        // System.out.println(pixelHsv[0] + "|" + pixelHsv[1] + "|" + pixelHsv[2]);
                    }
                }

                for (int i = 0; i < imageHsv.length; i++) {
                    hsvSignature[i] += imageHsv[i] / (image.getWidth() * image.getHeight());
                }

                count++;
            } catch (FileNotFoundException e) {
                logger.error("File not found: " + keyframeFile, e);
            } catch (IOException e) {
                logger.error("IO error when read image: " + keyframeFile, e);
            }
        }

        // 没有有效的关键帧,返回null
        if (count == 0) {
            return null;
        }

        for (int i = 0; i < hsvSignature.length; i++) {
            hsvSignature[i] /= count;

            // System.out.println(hsvSignature[i]);
        }

        HSVSigEntity signature = new HSVSigEntity(hsvSignature);

        return signature;
    }

    /**
     * Quantize the HSV signature
     *
     * @param pixelHsv the HSV of a pixel
     * @param imageHsv the quantized image HSV
     */
    public static void hsvQuantization(float[] pixelHsv, float[] imageHsv) {
        float h = pixelHsv[0] * 360;
        float s = pixelHsv[1] * 3;
        float v = pixelHsv[2] * 3;

        // 量化H分量,以20等间隔量化
        imageHsv[(int) h / 20]++;

        // 量化V分量,以1等间隔量化
        imageHsv[(int) s + 18]++;

        // 量化S分量,以1等间隔量化
        imageHsv[(v >= 3)
                 ? 23
                 : (int) v + 21]++;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        KeyFrameDao          dao          = new KeyFrameDao();
        List<KeyFrameEntity> keyframeList = dao.getKeyFrameByVideoId("773");
        HSVSigEntity         signature    = GlobalSigGenerator.generate(keyframeList);

        System.out.println(signature.getBin1());
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
