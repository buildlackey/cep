/*
 * Author: cbedford
 * Date: 11/1/13
 * Time: 9:58 PM
 */

import java.io.Serializable;

public interface IFeedItemProvider extends Serializable {
    Runnable getRunnableTask();
    Object getNextItemIfAvailable();
}
