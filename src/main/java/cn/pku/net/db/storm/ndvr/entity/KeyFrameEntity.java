/**
 * @Package cn.pku.net.db.storm.ndvr.entity
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.entity;

import java.util.Comparator;
import java.util.regex.Pattern;

/**
 * Description: Keyframe entity
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:40
 */
public class KeyFrameEntity implements Comparator<KeyFrameEntity> {
    private String keyFrameName;
    private String videoId;
    private String videoFileName;
    private String serialId;    // serial number of the keyframe in video

    /**
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(KeyFrameEntity o1, KeyFrameEntity o2) {
        int serialId1 = Integer.parseInt(o1.getSerialId());
        int serialId2 = Integer.parseInt(o2.getSerialId());

        if (serialId1 < serialId2) {
            return -1;
        } else if (serialId1 > serialId2) {
            return 1;
        }

        return 0;
    }

    /**
     * Parse keyframe entity from a line.
     *
     * @param infos the infos
     * @return the key frame entity
     */
    public static KeyFrameEntity parse(String[] infos) {
        KeyFrameEntity ent = null;

        if ((infos.length == 4) && isInteger(infos[0])) {
            ent = new KeyFrameEntity();
            ent.setSerialId(infos[0]);
            ent.setKeyFrameName(infos[1]);
            ent.setVideoId(infos[2]);
            ent.setVideoFileName(infos[3]);
        }

        return ent;
    }

    /**
     * is integer?
     *
     * @param str the string
     * @return the boolean
     */
    public static boolean isInteger(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");

        return pattern.matcher(str).matches();
    }

    /**
     * Getter method for property <tt>keyFrameName</tt>.
     *
     * @return property value of keyFrameName
     */
    public String getKeyFrameName() {
        return keyFrameName;
    }

    /**
     * Setter method for property <tt>keyFrameName</tt>.
     *
     * @param keyFrameName value to be assigned to property keyFrameName
     */
    public void setKeyFrameName(String keyFrameName) {
        this.keyFrameName = keyFrameName;
    }

    /**
     * Getter method for property <tt>serialId</tt>.
     *
     * @return property value of serialId
     */
    public String getSerialId() {
        return serialId;
    }

    /**
     * Setter method for property <tt>serialId</tt>.
     *
     * @param serialId value to be assigned to property serialId
     */
    public void setSerialId(String serialId) {
        this.serialId = serialId;
    }

    /**
     * Getter method for property <tt>videoFileName</tt>.
     *
     * @return property value of videoFileName
     */
    public String getVideoFileName() {
        return videoFileName;
    }

    /**
     * Setter method for property <tt>videoFileName</tt>.
     *
     * @param videoFileName value to be assigned to property videoFileName
     */
    public void setVideoFileName(String videoFileName) {
        this.videoFileName = videoFileName;
    }

    /**
     * Getter method for property <tt>videoId</tt>.
     *
     * @return property value of videoId
     */
    public String getVideoId() {
        return videoId;
    }

    /**
     * Setter method for property <tt>videoId</tt>.
     *
     * @param videoId value to be assigned to property videoId
     */
    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
