/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.util.ResourceUtil;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

/**
 * auto partition by Long ,can be used in auto increment primary key partition
 * 
 * @author wuzhi
 */
public class AutoPartitionByMutiCols extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final long serialVersionUID = 5752372920655270639L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoPartitionByMutiCols.class);
    private String ruleFile = null;
    private int defaultNode = -1;
    private int hashCode = 1;
    
    private String mapFile;
	private TableColConfig[] tableColConfigs;
	
	private int maxNodeIndex = -1;

    
    
    
    @Override
    public void init() {
        initialize();
        initHashCode();
    }

    @Override
    public void selfCheck() {

    }

    public void setMapFile(String mapFile) {
        this.mapFile = mapFile;
    }


    public void setRuleFile(String ruleFile) {
        this.ruleFile = ruleFile;
    }

    @Override
    public Integer calculate(String columnValue) {
        //columnValue = NumberParseUtil.eliminateQuote(columnValue);
        try {

//            if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
//                if (defaultNode >= 0) {
//                    return defaultNode;
//                }
//                return null;
//            }
//
//            long value = Long.parseLong(columnValue);
//            for (LongRange longRang : this.longRanges) {
//                if (value <= longRang.getValueEnd() && value >= longRang.getValueStart()) {
//                    return longRang.getNodeIndex();
//                }
//            }
//            // use default node for other value
//            if (defaultNode >= 0) {
//                return defaultNode;
//            }
            return null;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
        }
    }

    /**
     * @param columnValue
     * @return
     */
    public boolean isUseDefaultNode(String columnValue) {
//        try {
//            long value = Long.parseLong(columnValue);
//            for (LongRange longRang : this.longRanges) {
//                if (value <= longRang.getValueEnd() && value >= longRang.getValueStart()) {
//                    return false;
//                }
//            }
//            if (defaultNode >= 0) {
//                return true;
//            }
//        } catch (NumberFormatException e) {
//            throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
//        }
        return false;
    }


    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        Integer begin = 0, end = 0;
//        if (isUseDefaultNode(beginValue) || isUseDefaultNode(endValue)) {
//            begin = 0;
//            end = longRanges.length - 1;
//        } else {
//            begin = calculate(beginValue);
//            end = calculate(endValue);
//        }


        if (begin == null || end == null) {
            return new Integer[0];
        }
        if (end >= begin) {
            int len = end - begin + 1;
            Integer[] re = new Integer[len];

            for (int i = 0; i < len; i++) {
                re[i] = begin + i;
            }
            return re;
        } else {
            return new Integer[0];
        }
    }

    @Override
    public int getPartitionNum() {
    	return 0;
//        return longRanges.length;
    }


	/**
	 * 初始化方法
	 */
	private void initialize() {
		BufferedReader in = null;
		try {
			// FileInputStream fin = new FileInputStream(new File(fileMapPath));
			InputStream fin = this.getClass().getClassLoader()
					.getResourceAsStream(mapFile);
			if (fin == null) {
				throw new RuntimeException("can't find class resource file "
						+ mapFile);
			}
			in = new BufferedReader(new InputStreamReader(fin));
			LinkedList<TableColConfig> TableColConfigList = new LinkedList<TableColConfig>();

			for (String line = null; (line = in.readLine()) != null;) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//")) {
					continue;
				}
				String str[] = line.trim().split("::");
				if (str.length != 3) {
					System.out.println(" warn: bad line int " + mapFile + " :"
							+ line);
					continue;
				}
				String tableCol[] = str[0].trim().split(",");
				String tableName = tableCol[0];
				String col = tableCol[1];
				
				Integer colType = Integer.parseInt(str[1].trim());
				
				String valueRanges[] = str[2].trim().split(",");
				LinkedList<StrRange> strRangeList = new LinkedList<StrRange>();
				LinkedList<LongRange> longRangeList = new LinkedList<LongRange>();
				if(colType == 1 ) {
					for(String longRange : valueRanges) {
						int eqIdx = longRange.indexOf("=");
						String pairs[] = longRange.substring(0, eqIdx).trim().split("--");
						long longStart = NumberParseUtil.parseLong(pairs[0].trim());
						long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
						int nodeId = Integer.parseInt(longRange.substring(eqIdx + 1)
								.trim());
						longRangeList
								.add(new LongRange(nodeId, longStart, longEnd));
					}
				}else if(colType == 2) {
					
					for(String strRange : valueRanges) {
						int eqIdx = strRange.indexOf("=");
						String pairs[] = strRange.substring(0, eqIdx).trim().split("--");
						String strStart = pairs[0].trim();
						String strEnd = pairs[1].trim();
						int nodeId = Integer.parseInt(strRange.substring(eqIdx + 1)
								.trim());
						strRangeList.add(new StrRange(nodeId, strStart, strEnd));
					}
				}
				TableColConfigList.add(new TableColConfig(tableName, col, colType,longRangeList,strRangeList));
			}
			tableColConfigs = TableColConfigList.toArray(new TableColConfig[TableColConfigList.size()]);
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}

		} finally {
			try {
				in.close();
			} catch (Exception e2) {
			}
		}
	}
    
    

    public void setDefaultNode(int defaultNode) {
        if (defaultNode >= 0 || defaultNode == -1) {
            this.defaultNode = defaultNode;
        } else {
            LOGGER.warn("numberrange algorithm default node less than 0 and is not -1, use -1 replaced.");
        }
        propertiesMap.put("defaultNode", String.valueOf(defaultNode));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AutoPartitionByMutiCols other = (AutoPartitionByMutiCols) o;
        if (other.defaultNode != defaultNode) {
            return false;
        }
//        if (other.longRanges.length != longRanges.length) {
//            return false;
//        }
//        for (int i = 0; i < longRanges.length; i++) {
//            if (!other.longRanges[i].equals(longRanges[i])) {
//                return false;
//            }
//        }
        return true;
    }

    

	//自定义配置类，对应txt文件一行数据
	static class TableColConfig {
		public final String tableName;
		public final String colName;
		public final Integer colType;
		public final LinkedList<LongRange> longRangeList;
		public final LinkedList<StrRange> strRangeList;

		public TableColConfig(String tableName, String colName, Integer colType,LinkedList<LongRange> longRangeList,LinkedList<StrRange> strRangeList) {
			super();
			this.tableName = tableName;
			this.colName = colName;
			this.colType = colType;
			this.longRangeList = longRangeList;
			this.strRangeList = strRangeList;
		}
	}
	
	static class LongRange {
		public final int nodeIndx;
		public final long valueStart;
		public final long valueEnd;
		public LongRange(int nodeIndx, long valueStart, long valueEnd) {
			super();
			this.nodeIndx = nodeIndx;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
		}
	}
	static class StrRange {
		public final int nodeIndx;
		public final String strStart;
		public final String strEnd;
		public StrRange(int nodeIndx, String strStart, String strEnd) {
			super();
			this.nodeIndx = nodeIndx;
			this.strStart = strStart;
			this.strEnd = strEnd;
		}
	}
	
	@Override
    public int hashCode() {
        return hashCode;
    }

	private void initHashCode() {
        if (defaultNode != 0) {
            hashCode *= defaultNode;
        }
        
    }


	/**
	 * 自定义计算路由方法	
	 */
	@Override
	public Integer calculateRangePlus(String tableName, String colName, List<Object> values) {
		maxNodeIndex = -1;
		for (TableColConfig tableColConfig : this.tableColConfigs) {
			if (tableName.equals(tableColConfig.tableName.toUpperCase()) && colName.equals(tableColConfig.colName.toUpperCase())) {
				for (Object colValue  : values) {
					if (colValue != null) {
						if (colValue.getClass().equals(Integer.class) && tableColConfig.colType == 1) {
							long value = ((Integer) colValue).longValue();
							for (LongRange longRang : tableColConfig.longRangeList) {
								if (value <= longRang.valueEnd && value >= longRang.valueStart) {
									if (longRang.nodeIndx > maxNodeIndex) {
										maxNodeIndex = longRang.nodeIndx;
									}
								}
							}
						} else if (colValue.getClass().equals(String.class)  && tableColConfig.colType == 2) {
							String value = (String) colValue;
							for (StrRange strRang : tableColConfig.strRangeList) {
								if (value.compareTo(strRang.strStart.trim()) >= 0 && value.compareTo(strRang.strEnd.trim()) <= 0  ) {
									if (strRang.nodeIndx > maxNodeIndex) {
										maxNodeIndex = strRang.nodeIndx;
									}
								}
							}
						}
					}
				}
			}
		}
		// 数据超过范围，暂时使用配置的默认节点
		if (maxNodeIndex >= 0) {
			return maxNodeIndex;
		}else {
			return null;
		}
	}
	
}
