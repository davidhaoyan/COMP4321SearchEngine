package project;

import jdbm.RecordManager;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;

public class JDBMHelper {
    JDBMHelper() {}
    public static HTree createOrLoadHTree(RecordManager recordManager, String treeName) throws IOException {
        long recId = recordManager.getNamedObject(treeName);
        HTree htree;
        if (recId != 0) {
            htree = HTree.load(recordManager, recId);
        } else {
            htree = HTree.createInstance(recordManager);
            recordManager.setNamedObject(treeName, htree.getRecid());
        }
        return htree;
    }

    public static void printAll(HTree htree) throws IOException {
        FastIterator iter = htree.keys();
        Object key;
        while ((key=iter.next()) != null) {
            System.out.println(key+":"+htree.get(key));
        }
    }

    public static Integer calculateSizeHTree(HTree htree) throws IOException {
        FastIterator pageIterator = htree.keys();
        Integer n = 0;
        Object key;
        while ((key = pageIterator.next()) != null) {
            n++;
        }
        return n;
    }
}
