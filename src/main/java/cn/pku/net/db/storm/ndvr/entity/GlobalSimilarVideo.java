/**
 * @Package cn.pku.net.db.storm.ndvr.entity
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.entity;

import java.util.Comparator;

/**
 * Description: global similar video with id and distance
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:37
 */
public class GlobalSimilarVideo implements Comparator<GlobalSimilarVideo> {
    private String videoId;
    private float  globalSigEucliDistance;

    /**
     * Instantiates
     */
    public GlobalSimilarVideo() {}

    /**
     * Instantiates with fields
     *
     * @param videoId                the video id
     * @param globalSigEucliDistance the global sig Euclid distance
     */
    public GlobalSimilarVideo(String videoId, float globalSigEucliDistance) {
        super();
        this.videoId                = videoId;
        this.globalSigEucliDistance = globalSigEucliDistance;
    }

    /**
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(GlobalSimilarVideo arg0, GlobalSimilarVideo arg1) {
        float dist0 = arg0.getGlobalSigEucliDistance();
        float dist1 = arg1.getGlobalSigEucliDistance();

        if (dist0 < dist1) {
            return -1;
        } else if (dist0 > dist1) {
            return 1;
        }

        return 0;
    }

    /**
     * Getter method for property <tt>globalSigEucliDistance</tt>.
     *
     * @return property value of globalSigEucliDistance
     */
    public float getGlobalSigEucliDistance() {
        return globalSigEucliDistance;
    }

    /**
     * Setter method for property <tt>globalSigEucliDistance</tt>.
     *
     * @param globalSigEucliDistance value to be assigned to property globalSigEucliDistance
     */
    public void setGlobalSigEucliDistance(float globalSigEucliDistance) {
        this.globalSigEucliDistance = globalSigEucliDistance;
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
