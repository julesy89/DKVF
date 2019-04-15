/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.generator;

/**
 * An expression that generates a sequence of values, following some distribution (Uniform, Zipfian, Sequential, etc.).
 */
public abstract class Generator<V> {
  /**
   * Generate the next value in the distribution.
   */
  public abstract V nextValue();

  /**
   * Return the previous value generated by the distribution; e.g., returned from the last {@link Generator#nextValue()}
   *  call.
   * Calling {@link #lastValue()} should not advance the distribution or have any side effects. If {@link #nextValue()}
   * has not yet been called, {@link #lastValue()} should return something reasonable.
   */
  public abstract V lastValue();

  public final String nextString() {
    V ret = nextValue();
    return ret == null ? null : ret.toString();
  }

  public final String lastString() {
    V ret = lastValue();
    return ret == null ? null : ret.toString();
  }
}

