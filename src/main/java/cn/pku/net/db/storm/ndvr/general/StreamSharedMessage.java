/**
 * Created by jeremyjiang on 2016/5/13.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.general;

import cn.pku.net.db.storm.ndvr.common.Const;
import org.apache.log4j.Logger;

import java.util.*;

import static cn.pku.net.db.storm.ndvr.common.Const.SSM_CONFIG.DISCARD_KEYS;


/**
 * Description:
 *
 * @author jeremyjiang Created at 2016/5/13 14:47
 */
public class StreamSharedMessage {

    private static final Logger logger = Logger.getLogger(StreamSharedMessage.class);

    /**
     * Calculate the fields each component should discard
     *
     * @param topology the topology graph
     * @param needKeys the needed keys
     * @param newKeys  the new keys produced
     * @return the map
     */
    public static Map<String, Set<String>> calMsgReduction(Map<String, Set<String>> topology, Map<String, Set<String>> needKeys, Map<String, Set<String>> newKeys){
        if (topology.size() != needKeys.size() || topology.size() != newKeys.size()) {
            logger.error("Size cannot match");
            return null;
        }
        Set<String> visited = new HashSet<String>();    // component has been visited
        Set<String> unvisited = new HashSet<String>();  // component has not been visited
        unvisited.addAll(topology.keySet());

        Map<String, Set<String>> outputKeys = new HashMap<String, Set<String>>();

        // Calculate the output fields of each component
        for (int i = 0; i < topology.size(); i++) {
            final String component = selectNextComponent(topology, visited, unvisited);
            //System.out.println(String.format("------Select component %s", component));
            if (null == component) {
                break;
            }
            Set<String> curOutput = new HashSet<String>();
            if (topology.get(component).size() == 0){
                curOutput.addAll(newKeys.get(component));
            } else {
                for (String child : topology.get(component)) {
                    //System.out.println("Child: " + child);
                    if (needKeys.containsKey(child)) {
                        curOutput.addAll(needKeys.get(child));
                        //System.out.println("Add " + Arrays.toString(needKeys.get(child).toArray()));
                    }
                    if (outputKeys.containsKey(child)) {
                        curOutput.addAll(outputKeys.get(child));
                        //System.out.println("Add " + Arrays.toString(outputKeys.get(child).toArray()));
                    }
                }
                for (String child : topology.get(component)) {
                    if (newKeys.containsKey(child)) {
                        curOutput.removeAll(newKeys.get(child));
                        //System.out.println("Remove " + Arrays.toString(newKeys.get(child).toArray()));
                    }
                }
            }
            //System.out.println(String.format("Output keys of component %s: %s", component, Arrays.toString(curOutput.toArray())));
            outputKeys.put(component, curOutput);
        }

        Map<String, Set<String>> evictKeys = new HashMap<String, Set<String>>();
        Map<String, Set<String>> parents = new HashMap<String, Set<String>>();
        // Get the parent components of each component
        for (String component : topology.keySet()){
            for (String child : topology.get(component)){
                if (!parents.containsKey(child)){
                    parents.put(child, new HashSet<String>());
                }
                parents.get(child).add(component);
            }
        }
        // Calculate the fields which should be evicted
        for (String component : visited){
            if (!parents.containsKey(component)){
                continue;
            }
            Set<String> evict = new HashSet<String>();
            for (String parent : parents.get(component)){
                if (outputKeys.containsKey(parent)){
                    evict.addAll(outputKeys.get(parent));
                }
            }
            evict.removeAll(outputKeys.get(component));
            evictKeys.put(component, evict);
            System.out.println(component + " evict: " + Arrays.toString(evict.toArray()));
        }

        return evictKeys;
    }

    /**
     * Select the next component when calculate the discard fields
     *
     * @param topology  the topology
     * @param visited   the visited
     * @param unvisited the unvisited
     * @return the component string
     */
    public static String selectNextComponent(Map<String, Set<String>> topology, Set<String> visited, Set<String> unvisited){
        for (String parent : unvisited) {
            if (visited.containsAll(topology.get(parent)) || topology.get(parent).isEmpty()){
                visited.add(parent);
                unvisited.remove(parent);
                return parent;
            }
        }
        return null;
    }

    /**
     * Cal SSM's length in bytes
     *
     * @param ctrlMsg the SSM ctrl message
     * @return the SSM's length
     */
    public static int calMsgLength(Map<String, String> ctrlMsg) {
        int msgLength = 0;
        for (Map.Entry<String, String> entry : ctrlMsg.entrySet()) {
            msgLength += entry.getKey().length() + entry.getValue().length();
        }
        return msgLength;
    }

    /**
     * Discard invalid key map.
     *
     * @param componentName  the name of component
     * @param ctrlMsg the Stream Shared Message
     * @return the map
     */
    public static Map<String, String> discardInvalidKey(String componentName, Map<String, String> ctrlMsg) {
        if (Const.SSM_CONFIG.IS_REDUCTIION) {
            if (!DISCARD_KEYS.containsKey(componentName)) {
                return ctrlMsg;
            }
            Set<String> discaredKeys = DISCARD_KEYS.get(componentName);
            for (String key : discaredKeys) {
                ctrlMsg.remove(key);
            }
        }
        return ctrlMsg;
    }

    /**
     * Main.
     *
     * @param args the args
     */
    public static void main(String[] args){
        String[] components = {"GetTaskSpout", "GlobalSigBolt", "GlobalSigSimilarBolt", "GlobalResultBolt"};

        Map<String, Set<String>> topology = new HashMap<String, Set<String>>();
        topology.put("GetTaskSpout", new HashSet<String>(){ { add("GlobalSigBolt"); } });
        topology.put("GlobalSigBolt", new HashSet<String>(){ { add("GlobalSigSimilarBolt"); } });
        topology.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("GlobalResultBolt"); } });
        topology.put("GlobalResultBolt", new HashSet<String>());

        Map<String, Set<String>> needKeys = new HashMap<String, Set<String>>();    // fields needed by each component
        Map<String, Set<String>> newKeys = new HashMap<String, Set<String>>();    // fields produced by each component

        needKeys.put("GetTaskSpout", new HashSet<String>(){ { add("task"); } });
        newKeys.put("GetTaskSpout", new HashSet<String>(){ { add("queryVideo"); } });

        needKeys.put("GlobalSigBolt", new HashSet<String>(){ { add("queryVideo"); } });
        newKeys.put("GlobalSigBolt", new HashSet<String>(){ { add("keyframeList"); add("globalSignature"); } });

        needKeys.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("queryVideo"); add("globalSignature"); } });
        newKeys.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("globalSimilarVideoList"); } });

        //needKeys.put("LocalSigBolt", new HashSet<String>(){ { add("queryVideo"); add("keyframeList"); } });
        //newKeys.put("LocalSigBolt", new HashSet<String>(){ { add("localSignature"); } });
        //
        //needKeys.put("LocalSigSimilarBolt", new HashSet<String>(){ { add("queryVideo"); add("localSignature"); } });
        //newKeys.put("LocalSigSimilarBolt", new HashSet<String>(){ { add("localSimilarVideoList"); } });
        //
        //needKeys.put("TextSimilarBolt", new HashSet<String>(){ { add("queryVideo"); } });
        //newKeys.put("TextSimilarBolt", new HashSet<String>(){ { add("textSimilarVideoList"); } });

        needKeys.put("GlobalResultBolt", new HashSet<String>(){ { add("globalSimilarVideoList"); } });
        newKeys.put("GlobalResultBolt", new HashSet<String>(){ { add("result"); } });

        calMsgReduction(topology, needKeys, newKeys);
    }
}