package io.mycat.backend.mysql.nio.handler.builder;

import io.mycat.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.TableConfig.TableTypeEnum;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.node.TableNode;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TableNodeHandlerBuilder extends BaseHandlerBuilder {
    private TableNode node;
    private TableConfig tableConfig = null;

    protected TableNodeHandlerBuilder(NonBlockingSession session, TableNode node, HandlerBuilder hBuilder) {
        super(session, node, hBuilder);
        this.node = node;
        this.canPushDown = !node.existUnPushDownGroup();
        this.needWhereHandler = false;
        this.tableConfig = getTableConfig(node.getSchema(), node.getTableName());
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        return new ArrayList<>();
    }

    @Override
    public void buildOwn() {
        try {
            PushDownVisitor pdVisitor = new PushDownVisitor(node, true);
            MergeBuilder mergeBuilder = new MergeBuilder(session, node, needCommon, needSendMaker, pdVisitor);
            RouteResultsetNode[] rrssArray = mergeBuilder.construct().getNodes();
            this.needCommon = mergeBuilder.getNeedCommonFlag();
            this.needSendMaker = mergeBuilder.getNeedSendMakerFlag();
            buildMergeHandler(node, rrssArray);
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "tablenode buildOwn exception!", e);
        }
    }

    @Override
    protected void nestLoopBuild() {
        try {
            List<Item> filters = node.getNestLoopFilters();
            PushDownVisitor pdVisitor = new PushDownVisitor(node, true);
            if (filters == null || filters.isEmpty())
                throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "unexpected exception!");
            List<RouteResultsetNode> rrssList = new ArrayList<>();
            MergeBuilder mergeBuilder = new MergeBuilder(session, node, needCommon, needSendMaker, pdVisitor);
            if (tableConfig == null || tableConfig.getTableType() == TableTypeEnum.TYPE_GLOBAL_TABLE) {
                for (Item filter : filters) {
                    node.setWhereFilter(filter);
                    RouteResultsetNode[] rrssArray = mergeBuilder.construct().getNodes();
                    rrssList.addAll(Arrays.asList(rrssArray));
                }
                if (filters.size() == 1) {
                    this.needCommon = false;
                    this.needSendMaker = mergeBuilder.getNeedSendMakerFlag();
                }
            } else {
                boolean tryGlobal = filters.size() == 1;
                for (Item filter : filters) {
                    node.setWhereFilter(filter);
                    pdVisitor.visit();
                    RouteResultsetNode[] rrssArray = mergeBuilder.construct().getNodes();
                    rrssList.addAll(Arrays.asList(rrssArray));
                }
                if (tryGlobal) {
                    this.needCommon = mergeBuilder.getNeedCommonFlag();
                    this.needSendMaker = mergeBuilder.getNeedSendMakerFlag();
                }
            }
            RouteResultsetNode[] rrssArray = new RouteResultsetNode[rrssList.size()];
            rrssArray = rrssList.toArray(rrssArray);
            buildMergeHandler(node, rrssArray);
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "", e);
        }
    }

}
