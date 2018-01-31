package hudson.slaves;

import hudson.slaves.NodeProvisioner;
import hudson.slaves.Cloud;
import hudson.slaves.CloudProvisioningListener;

import hudson.AbortException;
import hudson.ExtensionPoint;
import hudson.model.*;
import jenkins.model.Jenkins;

import static hudson.model.LoadStatistics.DECAY;
import hudson.model.MultiStageTimeSeries.TimeScale;
import hudson.Extension;
import jenkins.util.SystemProperties;
import org.jenkinsci.Symbol;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import java.awt.Color;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.IOException;

/**
 * The aggresive strategy for container cloud.
 * don't wait for slave to complete for any reason. i.e, provision whenever queue_length greater than (available + connecting + plan)
 *
 */
@Extension @Symbol("kubernetes")
public class KubernetesStrategy extends NodeProvisioner.Strategy {

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public NodeProvisioner.StrategyDecision apply(@Nonnull NodeProvisioner.StrategyState state) {
        /*
            So this computation involves three stats:

              1. # of available executors
              2. # of jobs that are starving for executors
              3. # of additional agents being provisioned (planned capacities.)
              4. # of connecting agents

            we will only use the snapshot value, NOT considering EMA (exponential moving average)
         */

        final LoadStatistics.LoadStatisticsSnapshot snapshot = state.getSnapshot();
        LOGGER.log(Level.INFO, "stat snapshot = " + snapshot);
        LOGGER.log(Level.INFO, "state = " + state);

        boolean needSomeWhenNoneAtAll = (snapshot.getAvailableExecutors() + snapshot.getConnectingExecutors() == 0)
                && (snapshot.getOnlineExecutors() + state.getPlannedCapacitySnapshot() + state.getAdditionalPlannedCapacity() == 0)
                && (snapshot.getQueueLength() > 0);
        int qlen = snapshot.getQueueLength();

        int available = snapshot.getAvailableExecutors();
        int connectingCapacity = snapshot.getConnectingExecutors();
        // this is the additional executors we've already provisioned.
        int plannedCapacity = state.getPlannedCapacitySnapshot() + state.getAdditionalPlannedCapacity();

        int excessWorkload = qlen - available - plannedCapacity - connectingCapacity;

        if ( excessWorkload > 0 || needSomeWhenNoneAtAll) {
            if (excessWorkload < 1) {
                // in this specific exceptional case we should just provision right now
                // the exponential smoothing will delay the build unnecessarily
                excessWorkload = 1;
            }

            LOGGER.log(Level.INFO, "Excess workload {0} detected. " +
                            "(Qlen={1}, " +
                            "planned capacity={2}, " +
                            "connecting capacity={3}, " +
                            "available={4}, " +
                            "online={5})",
                    new Object[]{
                            excessWorkload, qlen, plannedCapacity, connectingCapacity, available,
                            snapshot.getOnlineExecutors()});

            if (excessWorkload > 0) {
                CLOUD:
                for (Cloud c : Jenkins.getInstance().clouds) {
                    if (excessWorkload < 0) {
                        break;  // enough agents allocated
                    }

                    // Make sure this cloud actually can provision for this label.
                    if (c.canProvision(state.getLabel())) {
                        int workloadToProvision = excessWorkload;

                        for (CloudProvisioningListener cl : CloudProvisioningListener.all()) {
                            if (cl.canProvision(c, state.getLabel(), workloadToProvision) != null) {
                                // consider displaying reasons in a future cloud ux
                                continue CLOUD;
                            }
                        }

                        Collection<NodeProvisioner.PlannedNode> additionalCapacities =
                                c.provision(state.getLabel(), workloadToProvision);

                        fireOnStarted(c, state.getLabel(), additionalCapacities);

                        for (NodeProvisioner.PlannedNode ac : additionalCapacities) {
                            excessWorkload -= ac.numExecutors;
                            LOGGER.log(Level.INFO, "Started provisioning {0} from {1} with {2,number,integer} "
                                            + "executors. Remaining excess workload: {3,number,#.###}",
                                    new Object[]{ac.displayName, c.name, ac.numExecutors, excessWorkload});
                        }
                        state.recordPendingLaunches(additionalCapacities);
                    }
                }
                // we took action, only pass on to other strategies if our action was insufficient
                return excessWorkload > 0 ? NodeProvisioner.StrategyDecision.CONSULT_REMAINING_STRATEGIES : NodeProvisioner.StrategyDecision.PROVISIONING_COMPLETED;
            }
            else {
                LOGGER.log(Level.INFO, "no excessWorkload, no need to provision new worker");
            }
        }
        else {
            LOGGER.log(Level.INFO, "available >= queue, no need to provision new worker");
        }

        // if we reach here then the standard strategy obviously decided to do nothing, so let any other strategies
        // take their considerations.
        return NodeProvisioner.StrategyDecision.CONSULT_REMAINING_STRATEGIES;
    }

    private static final Logger LOGGER = Logger.getLogger(KubernetesStrategy.class.getName());

    private static void fireOnStarted(final Cloud cloud, final Label label,
                                      final Collection<NodeProvisioner.PlannedNode> plannedNodes) {
        for (CloudProvisioningListener cl : CloudProvisioningListener.all()) {
            try {
                cl.onStarted(cloud, label, plannedNodes);
            } catch (Error e) {
                throw e;
            } catch (Throwable e) {
                LOGGER.log(Level.SEVERE, "Unexpected uncaught exception encountered while "
                        + "processing onStarted() listener call in " + cl + " for label "
                        + label.toString(), e);
            }
        }
    }
}

