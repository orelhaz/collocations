package writeableClasses;

import org.apache.hadoop.io.WritableComparator;


import org.apache.hadoop.io.WritableComparable;

public class DecadeCountComparator extends WritableComparator {

        protected DecadeCountComparator() {
            super(DecadeCount.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DecadeCount k1 = (DecadeCount) w1;
            DecadeCount k2 = (DecadeCount)w2;

            return -1 * k1.compareTo(k2);
        }
    }
