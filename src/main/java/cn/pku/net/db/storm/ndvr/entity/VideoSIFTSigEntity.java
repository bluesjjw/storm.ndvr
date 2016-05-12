/**
 * @Package cn.pku.net.db.storm.ndvr.entity
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: Video SIFT signature
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:50
 */
public class VideoSIFTSigEntity {
    private List<SIFTSigEntity> signatures = new ArrayList<SIFTSigEntity>();    // 每个帧一个标签
    private String              videoId;

    /**
     * Instantiates with fields
     *
     * @param videoId   the video id
     * @param signature the signature
     */
    public VideoSIFTSigEntity(String videoId, List<SIFTSigEntity> signature) {
        super();
        this.videoId    = videoId;
        this.signatures = signature;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {}

    /**
     * Getter method for property <tt>signature</tt>.
     *
     * @return property value of signature
     */
    public List<SIFTSigEntity> getSignature() {
        return signatures;
    }

    /**
     * Setter method for property <tt>signature</tt>.
     *
     * @param signature value to be assigned to property signature
     */
    public void setSignature(List<SIFTSigEntity> signature) {
        this.signatures = signature;
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
