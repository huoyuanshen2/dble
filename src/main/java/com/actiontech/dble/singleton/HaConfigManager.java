package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.HaChangeStatus;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.config.loader.console.ZookeeperPath;
import com.actiontech.dble.config.loader.zkprocess.entity.Schemas;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.DataHost;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.ReadHost;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.WriteHost;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.xml.SchemasParseXmlImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DataSourceStatus;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.config.util.SchemaWriteJob;
import com.actiontech.dble.util.ResourceUtil;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH.JSON_LIST;
import static com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH.JSON_NAME;
import static com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH.JSON_WRITE_SOURCE;

/**
 * Created by szf on 2019/10/23.
 */
public final class HaConfigManager {
    private static final String HA_LOG = "ha_log";
    private static final Logger HA_LOGGER = LoggerFactory.getLogger(HA_LOG);
    private static final HaConfigManager INSTANCE = new HaConfigManager();
    private ParseXmlServiceInf<Schemas> parseSchemaXmlService;
    private static final String WRITEPATH = "schema.xml";
    private Schemas schema;
    private AtomicInteger indexCreater = new AtomicInteger();
    private final AtomicBoolean isWriting = new AtomicBoolean(false);
    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();
    private volatile SchemaWriteJob schemaWriteJob;
    private volatile Set<PhysicalDNPoolSingleWH> waitingSet = new HashSet<>();
    private final Map<Integer, HaChangeStatus> unfinised = new ConcurrentHashMap<>();

    private HaConfigManager() {
        try {
            XmlProcessBase xmlProcess = new XmlProcessBase();
            xmlProcess.addParseClass(Schemas.class);
            xmlProcess.initJaxbClass();
            this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlProcess);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void init() {
        INSTANCE.schema = this.parseSchemaXmlService.parseXmlToBean(WRITEPATH);
        return;
    }

    public void write(Schemas schemas) {
        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;
        this.parseSchemaXmlService.parseToXmlWrite(schemas, path, "schema");
    }

    public void finishAndNext() {
        isWriting.set(false);
        triggerNext();
    }

    public void updateConfDataHost(PhysicalDNPoolSingleWH physicalDNPoolSingleWH, boolean syncWriteConf) {
        SchemaWriteJob thisTimeJob = null;
        //check if there is one thread is writing
        if (isWriting.compareAndSet(false, true)) {
            adjustLock.writeLock().lock();
            try {
                waitingSet.add(physicalDNPoolSingleWH);
                schemaWriteJob = new SchemaWriteJob(waitingSet, schema);
                thisTimeJob = schemaWriteJob;
                waitingSet = new HashSet<>();
                DbleServer.getInstance().getComplexQueryExecutor().execute(schemaWriteJob);
            } finally {
                adjustLock.writeLock().unlock();
            }
        } else {
            adjustLock.readLock().lock();
            try {
                thisTimeJob = schemaWriteJob;
                waitingSet.add(physicalDNPoolSingleWH);
            } finally {
                adjustLock.readLock().unlock();
            }
            triggerNext();
        }
        if (syncWriteConf) {
            //waitDone
            thisTimeJob.waitForWritingDone();
        }
    }


    public void triggerNext() {
        if (waitingSet.size() != 0 && isWriting.compareAndSet(false, true)) {
            adjustLock.writeLock().lock();
            try {
                schemaWriteJob = new SchemaWriteJob(waitingSet, schema);
                waitingSet = new HashSet<>();
                DbleServer.getInstance().getComplexQueryExecutor().execute(schemaWriteJob);
            } finally {
                adjustLock.writeLock().unlock();
            }
        }
    }

    public Map<String, String> getSourceJsonList() {
        Map<String, String> map = new HashMap<>();
        for (DataHost dh : schema.getDataHost()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(JSON_NAME, dh.getName());
            List<DataSourceStatus> list = new ArrayList<>();
            for (WriteHost wh : dh.getWriteHost()) {
                list.add(new DataSourceStatus(wh.getHost(), "true".equals(wh.getDisabled()), false));
                jsonObject.put(JSON_WRITE_SOURCE, new DataSourceStatus(wh.getHost(), "true".equals(wh.getDisabled()), false));
                for (ReadHost rh : wh.getReadHost()) {
                    list.add(new DataSourceStatus(rh.getHost(), "true".equals(rh.getDisabled()), true));
                }
            }
            jsonObject.put(JSON_LIST, list);
            map.put(dh.getName(), jsonObject.toJSONString());
        }
        return map;
    }

    public static HaConfigManager getInstance() {
        return INSTANCE;
    }

    public int haStart(HaInfo.HaStage stage, HaInfo.HaStartType type, String command) {
        int id = indexCreater.getAndAdd(1);
        HaChangeStatus newStatus = new HaChangeStatus(id, command, type, System.currentTimeMillis(), stage);
        INSTANCE.unfinised.put(Integer.valueOf(id), newStatus);
        HA_LOGGER.info(new StringBuilder().
                append("[HA] id = ").append(id).
                append(" start of Ha event type =").append(type.toString()).
                append(" command = ").append(command).
                append(" stage = ").append(stage.toString()).
                toString());
        return id;
    }

    public void haFinish(int id, String errorMsg, String result) {
        HaChangeStatus status = unfinised.get(Integer.valueOf(id));
        unfinised.remove(Integer.valueOf(id));
        StringBuilder resultString = new StringBuilder().
                append("[HA] id = ").append(id).
                append(" end of Ha event type =").append(status.getType().toString()).
                append(" command = ").append(status.getCommand()).
                append(" stage = ").append(status.getStage().toString()).
                append(" finish type = \"").append(errorMsg == null ? "success" : errorMsg).append("\"");
        if (result != null) {
            resultString.append("\n result status of dataHost :").append(result);
        }
        if (errorMsg == null) {
            HA_LOGGER.info(resultString.toString());
        } else {
            HA_LOGGER.warn(resultString.toString());
        }
    }

    public void haWaitingOthers(int id) {
        HaChangeStatus status = unfinised.get(Integer.valueOf(id));
        status.setStage(HaInfo.HaStage.WAITING_FOR_OTHERS);
        HA_LOGGER.info(new StringBuilder().
                append("[HA] id = ").append(id).
                append(" ha waiting for others type =").append(status.getType().toString()).
                append(" command = ").append(status.getCommand()).
                append(" stage = ").append(status.getStage().toString()).
                toString());
    }

    public Map<Integer, HaChangeStatus> getUnfinised() {
        return unfinised;
    }



}
