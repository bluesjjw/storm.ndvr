/**
 * @Package cn.pku.net.db.storm.ndvr.common
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Description: Constant configuration
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:05
 */
public class Const {
    public static int THREAD_POOL_MAX_NUM = 10;    // the max number in thread pool

    /**
     * CC_WEB_VIDEO dataset related config
     */
    public static class CC_WEB_VIDEO {

        // PC
        // public static String VIDEO_INFO_PATH            = "E:\\dataset\\cc_web_video\\Video_Complete.txt";
        // public static String KEYFRAME_INFO_PATH         = "E:\\dataset\\cc_web_video\\Shot_Info.txt";
        // public static String HSV_SIGNATURE_PATH  = "E:\\dataset\\cc_web_video\\HSV.txt";
        // public static String KEYFRAME_PATH_PREFIX  = "E:\\dataset\\cc_web_video\\keyframe\\";
        // public static String SIFT_SIGNATURE_PATH_PREFIX = "E:\\dataset\\cc_web_video\\siftsignature\\";
        // Server
        public static String VIDEO_INFO_PATH            = "/home/jiangjw/dataset/cc_web_video/Video_Complete.txt";    // 视频元数据文件
        public static String KEYFRAME_INFO_PATH         = "/home/jiangjw/dataset/cc_web_video/Shot_Info.txt";    // 关键帧元数据文件
        public static String VIDEO_PATH_PREFIX          = "/home/jiangjw/dataset/cc_web_video/video/";    // 视频文件目录
        public static String KEYFRAME_PATH_PREFIX       = "/home/jiangjw/dataset/cc_web_video/keyframe/";    // 关键帧文件目录
        public static String HSV_SIGNATURE_PATH         = "/home/jiangjw/dataset/cc_web_video/HSV.txt";    // HSV标签文件
        public static String SIFT_SIGNATURE_PATH_PREFIX = "/home/jiangjw/dataset/cc_web_video/siftsignature/";    // SIFT标签文件目录
    }

    /**
     * MongoDB related config
     */
    public static class MONGO {
        public static String MONGO_TASK_HOST              = "162.105.146.209";    // MongoDB host sending tasks
        public static String MONGO_HOST                   = "localhost";          // MongoDB host
        public static int    MONGO_PORT                   = 27017;                // MongoDB port
        public static String MONGO_DATABASE               = "NDVR";               // MongoDB database name
        public static String MONGO_TASK_COLLECTION        = "task";               // task collection's name
        public static String MONGO_VIDEO_COLLECTION       = "videoInfo";          // video info collection's name
        public static String MONGO_KEYFRAME_COLLECTION    = "keyFrame";           // keyframe collection's name
        public static String MONGO_HSVSIG_COLLECTION      = "hsvSignature";       // HSV signature collection's name
        public static String MONGO_TASK_RESULT_COLLECTION = "taskResult";         // task result collection's name
    }


    /**
     * Storm related config.
     */
    public static class STORM_CONFIG {
        public static boolean IS_LOCAL_MODE                          = false;          // local mode switch
        public static boolean IS_DEBUG                               = true;           // debug log switch
        public static int     GET_TASK_INTERVAL                      = 100;            // task interval, in milliseconds
        public static String  RETRIEVAL_TASK_FLAG                    = "retrieval";    // the retrieval task flag
        public static String  DETECTION_TASK_FLAG                    = "detection";    // the detection task flag
        public static int     DETECTION_PAIRWISE_THRESHOLD           = 100;            // the pair-wise count
        public static int     TEXT_COMPARED_WINDOW                   = 3;              // 比较两个视频文本信息时,单词的窗口大小
        public static int     VIDEO_DURATION_WINDOW                  = 10;             // 确定待比较的视频候选集时,视频时长的比较窗口大小
        public static int     BOLT_DURATION_WINDOW                   = 60;             // 每个bolt负责一段时间的视频,单位为秒
        public static int     FRAME_COMPARED_WINDOW                  = 3;              // 比较local标签时,帧间比较窗口的大小
        public static float   GLOBALSIG_EUCLIDEAN_THRESHOLD          = (float) 0.8;    // 全局标签的欧氏距离阈值
        public static float   GLOBALSIG_EUCLIDEAN_TRUST_THRESHOLD    = (float) 0.2;    // 全局标签的欧氏距离的可信阈值,小于此阈值就可以认为是近似重复视频
        public static int     LOCALSIG_KEYFRAME_MAXNUM               = 10;    // 计算SIFT标签时,最多使用前几个帧,避免有的视频帧太多,导致计算时间太长
        public static float   TEXT_SIMILARITY_THRESHOLD              = (float) 0.5;    // 文本信息相似度的阈值
        public static float   LOCALSIG_KEYFRAME_SIMILARITY_THRESHOLD = (float) 0.2;    // 两个帧图像之间,相似的SIFT keypoints数量达到一定比例即认为两个帧图像相似
        public static float   LOCALSIG_VIDEO_SIMILARITY_THRESHOLd = (float) 0.5;    // 两个视频之间,相似的帧图像数量达到一定的比例即认为两个视频相似
        public static boolean IS_FILTER_AND_REFINE                = true;           // 在使用多种特征时,是否是filter-and-refine的策略
    }

    /**
     * Shared Stream Message related config.
     */
    public static class SSM_CONFIG {
        public static boolean IS_REDUCTIION = true;    // SSM reduction switch, 是否缩减控制信息

        /**
         * Storm Topology graph
         */
        public static Map<String, Set<String>> STORM_TOPOLOGY = new HashMap<String, Set<String>>();

        /**
         * Component names in the Storm topology
         */
        public static String[] TOPOLOGY_COMPONENT = {"GetTaskSpout", "TextSimilarBolt", "GlobalSigBolt", "GlobalSigSimilarBolt",
                "LocalSigBolt", "LocalSigSimilarBolt", "TextResultBolt", "GlobalResultBolt",
                "TextGlobalPreResultBolt", "TextGlobalPostResultBolt", "GlobalLocalPreResultBolt"};

        public static Map<String, Set<String>> NEED_KEYS = new HashMap<String, Set<String>>();    // fields needed by each component
        public static Map<String, Set<String>> NEW_KEYS = new HashMap<String, Set<String>>();    // fields produced by each component
        public static Map<String, Set<String>> DISCARD_KEYS = new HashMap<String, Set<String>>();    // fields should discarded by each component

        static {
            /*--- the 1st application ---*/
            // textual signature
            // define the Storm topology
            //STORM_TOPOLOGY.put("GetTaskSpout", new HashSet<String>(){ { add("TextSimilarBolt"); } });
            //STORM_TOPOLOGY.put("TextSimilarBolt", new HashSet<String>(){ { add("TextResultBolt"); } });
            //STORM_TOPOLOGY.put("TextResultBolt", new HashSet<String>());
            //// define the needed fields of each component
            //NEED_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("task"); } });
            //NEED_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); } });
            //NEED_KEYS.put("TextResultBolt", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); add("textSimilarity"); } });
            //// define the produced fields of each component
            //NEW_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); } });
            //NEW_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("textSimilarity"); } });
            //NEW_KEYS.put("TextResultBolt", new HashSet<String>(){ { add("result"); } });

            ///*--- the 2nd application ---*/
            ////textual and global visual signature, post-filtering
            //STORM_TOPOLOGY.put("GetTaskSpout", new HashSet<String>(){ { add("TextSimilarBolt"); add("GlobalSigBolt"); } });
            //STORM_TOPOLOGY.put("TextSimilarBolt", new HashSet<String>(){ { add("TextGlobalPostResultBolt"); } });
            //STORM_TOPOLOGY.put("GlobalSigBolt", new HashSet<String>(){ { add("GlobalSigSimilarBolt"); } });
            //STORM_TOPOLOGY.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("TextGlobalPostResultBolt"); } });
            //// define the needed fields of each component
            //NEED_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("task"); } });
            //NEED_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("queryVideo"); add("queryVideo2"); } });
            //NEED_KEYS.put("TextResultBolt", new HashSet<String>(){ { add("queryVideo"); add("queryVideo2"); add("textSimilarity"); } });
            //// define the produced fields of each component
            //NEW_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("queryVideo"); add("queryVideo2"); } });
            //NEW_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("textSimilarity"); } });
            //NEW_KEYS.put("TextResultBolt", new HashSet<String>(){ { add("result"); } });
            //
            //// define the Storm topology
            //STORM_TOPOLOGY.put("GetTaskSpout", new HashSet<String>(){ { add("GlobalSigBolt"); } });
            //STORM_TOPOLOGY.put("GlobalSigBolt", new HashSet<String>(){ { add("GlobalSigSimilarBolt"); } });
            //STORM_TOPOLOGY.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("GlobalResultBolt"); } });
            //STORM_TOPOLOGY.put("GlobalResultBolt", new HashSet<String>());
            //// define the needed fields of each component
            //NEED_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("task"); } });
            //NEED_KEYS.put("GlobalSigBolt", new HashSet<String>(){ { add("queryVideo"); } });
            //NEED_KEYS.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("queryVideo"); add("globalSignature"); } });
            //NEED_KEYS.put("GlobalResultBolt", new HashSet<String>(){ { add("globalSimilarVideoList"); } });
            //// define the produced fields of each component
            //NEW_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("queryVideo"); } });
            //NEW_KEYS.put("GlobalSigBolt", new HashSet<String>(){ { add("keyframeList"); add("globalSignature"); } });
            //NEW_KEYS.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("globalSimilarVideoList"); } });
            //NEW_KEYS.put("GlobalResultBolt", new HashSet<String>(){ { add("result"); } });

            // calculate the fields need to be discard
            //DISCARD_KEYS = StreamSharedMessage.calMsgReduction(STORM_TOPOLOGY, NEED_KEYS, NEW_KEYS);
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        System.out.println("Test");
        for (Map.Entry<String, Set<String>> entry : Const.SSM_CONFIG.DISCARD_KEYS.entrySet()) {
            System.out.println(String.format("Component: %s, discard fields: %s", entry.getKey(), Arrays.toString(entry.getValue().toArray())));
        }
    }

}

//~ Formatted by Jindent --- http://www.jindent.com
