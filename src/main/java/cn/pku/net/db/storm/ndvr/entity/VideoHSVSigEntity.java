/**
 * @Package cn.pku.net.db.storm.ndvr.entity
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.entity;

/**
 * Description: Video HSV signature
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:48
 */
public class VideoHSVSigEntity {
    private String       videoId;
    private HSVSigEntity signature;

    /**
     * Instantiates
     */
    public VideoHSVSigEntity() {}

    /**
     * Instantiates with fields
     *
     * @param lines the lines
     */
    public VideoHSVSigEntity(String[] lines) {
        this.videoId   = lines[0];
        this.signature = new HSVSigEntity(lines);
    }

    /**
     * Instantiates a new Video hsv sig entity.
     *
     * @param vid    the vid
     * @param sigEnt the sig ent
     */
    public VideoHSVSigEntity(String vid, HSVSigEntity sigEnt) {
        this.videoId   = vid;
        this.signature = sigEnt;
    }

    /**
     * Getter method for property <tt>sig</tt>.
     *
     * @return property value of sig
     */
    public HSVSigEntity getSig() {
        return signature;
    }

    /**
     * Setter method for property <tt>sig</tt>.
     *
     * @param sig value to be assigned to property sig
     */
    public void setSig(HSVSigEntity sig) {
        this.signature = sig;
    }

    /**
     * Getter method for property <tt>vid</tt>.
     *
     * @return property value of vid
     */
    public String getVideoId() {
        return videoId;
    }

    /**
     * Setter method for property <tt>vid</tt>.
     *
     * @param videoId the video id
     */
    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
