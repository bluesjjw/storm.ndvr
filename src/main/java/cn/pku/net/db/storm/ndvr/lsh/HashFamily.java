/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

import java.io.Serializable;

/**
 * Description:  An interface representing a family of hash functions. A hash family has the
 * ability to generate a new member of the family, and can combine hashes
 * generated by a family of hash functions.
 *
 * @author jeremyjiang
 * Created at 2016/6/17 10:55
 */
public interface HashFamily extends Serializable{

    /**
     * Create a new hash function of this family.
     *
     * @return A new hash function of this family.
     */
    HashFunction createHashFunction();

    /**
     * Combine a number of hashes generated by members of this hash function
     * family.
     *
     * @param hashes
     *            The raw hashes that need to be combined.
     * @return An integer representing a combination of the hashes. Normally,
     *         unique hash values result in a unique, deterministic combined
     *         hash value.
     */
    Integer combine(int[] hashes);

    /**
     * Create a new distance measure.
     *
     * @return The distance measure used to sort neighbourhood candidates.
     */
    DistanceMeasure createDistanceMeasure();
}
