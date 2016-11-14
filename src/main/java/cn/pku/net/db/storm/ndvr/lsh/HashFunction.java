/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

import java.io.Serializable;

/**
 * Description: A hash function can hash a vector of arbitrary dimensions to an integer
 * representation. The hash function needs to be locality sensitive to work in
 * the locality sensitive hash scheme. Meaning that vectors that are 'close'
 * according to some metric have a high probability to end up with the same
 * hash.
 *
 * @author jeremyjiang
 * Created at 2016/6/17 10:41
 */
public interface HashFunction extends Serializable{

    /**
     * Hashes a vector of arbitrary dimensions to an integer. The hash function
     * needs to be locality sensitive to work in the locality sensitive hash (LSH)
     * scheme. Meaning that vectors that are 'close' according to some metric
     * have a high probability to end up with the same hash.
     *
     * @param vector
     *            The vector to hash. Can have any number of dimensions.
     * @return A locality sensitive hash (LSH). Vectors that are 'close'
     *         according to some metric have a high probability to end up with
     *         the same hash.
     */
    int hash(Vector vector);
}
