package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;


/**
 * Created by szf on 2019/10/22.
 */
public final class DataHostEnable {

    private DataHostEnable() {

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DataHostEnable.class);

    public static void execute(Matcher enable, ManagerConnection mc) {
        String dhName = enable.group(1);
        String subHostName = enable.group(3);
        boolean useCluster = ClusterHelper.useCluster();
        //check the dataHost is exists

        AbstractPhysicalDBPool dataHost = DbleServer.getInstance().getConfig().getDataHosts().get(dhName);
        if (dataHost == null) {
            mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
            return;
        }

        if (dataHost instanceof PhysicalDNPoolSingleWH) {
            PhysicalDNPoolSingleWH dh = (PhysicalDNPoolSingleWH) dataHost;
            if (!dh.checkDataSourceExist(subHostName)) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Some of the dataSource in command in " + dh.getHostName() + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, enable.group(0));
            if (ClusterGeneralConfig.isUseGeneralCluster() && useCluster) {
                if (!enableWithCluster(id, dh, subHostName, mc)) {
                    return;
                }
            } else if (ClusterGeneralConfig.isUseZK() && useCluster) {
                if (!enableWithZK(id, dh, subHostName, mc)) {
                    return;
                }
            } else {
                String result = dh.enableHosts(subHostName, true);
                HaConfigManager.getInstance().haFinish(id, null, result);
            }


            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.write(mc);
        } else {
            mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
        }
    }


    public static boolean enableWithZK(int id, PhysicalDNPoolSingleWH dh, String subHostName, ManagerConnection mc) {
        CuratorFramework zkConn = ZKUtils.getConnection();
        InterProcessMutex distributeLock = new InterProcessMutex(zkConn, KVPathUtil.getHaLockPath(dh.getHostName()));
        try {
            try {
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is change the dataHost status");
                    return false;
                }
                String result = dh.enableHosts(subHostName, false);
                DataHostDisable.setStatusToZK(KVPathUtil.getHaStatusPath(dh.getHostName()), zkConn, result);
                HaConfigManager.getInstance().haFinish(id, null, result);
            } finally {
                distributeLock.release();
                LOGGER.info("reload config: release distributeLock " + KVPathUtil.getConfChangeLockPath() + " from zk");
            }
        } catch (Exception e) {
            LOGGER.info("reload config using ZK failure", e);
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
            return false;
        }
        return true;
    }


    public static boolean enableWithCluster(int id, PhysicalDNPoolSingleWH dh, String subHostName, ManagerConnection mc) {
        //get the lock from ucore
        DistributeLock distributeLock = new DistributeLock(ClusterPathUtil.getHaLockPath(dh.getHostName()),
                new HaInfo(dh.getHostName(),
                        ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                        HaInfo.HaType.DATAHOST_ENABLE,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        try {
            if (!distributeLock.acquire()) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is changing the dataHost, please try again later.");
                return false;
            }
            String result = dh.enableHosts(subHostName, false);
            //only update for the status
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getHostName()), result);
            HaConfigManager.getInstance().haFinish(id, null, result);
        } catch (Exception e) {
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            return false;
        } finally {
            distributeLock.release();
        }
        return true;
    }
}
