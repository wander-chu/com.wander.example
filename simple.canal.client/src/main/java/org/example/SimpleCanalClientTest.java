package com.wander.example;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

public class SimpleCanalClientTest {
  private static final Logger logger = LoggerFactory.getLogger(SimpleCanalClientTest.class);
  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
  protected static final String LINE_SEPARATOR = SystemUtils.LINE_SEPARATOR;
  private static final String context_format = LINE_SEPARATOR
          + "**************************************************************************" + LINE_SEPARATOR
          + "* Batch Id: [{}], count: [{}], mem size: [{}] , time: {}" + LINE_SEPARATOR
          + "* Start : [{}]" + LINE_SEPARATOR
          + "* End : [{}]" + LINE_SEPARATOR
          + "**************************************************************************" + LINE_SEPARATOR;

  public static void main(String[] args) {
    SimpleCanalClientTest example = new SimpleCanalClientTest();
    example.testCanalClient();
  }

  private void testCanalClient() {
    String address = "172.24.8.181";
    int port = 11111;
    InetSocketAddress socketAddress = new InetSocketAddress(address, port);
    String destination = "example";
    String username = "canal";
    String password = "canal";
    CanalConnector connector = CanalConnectors.newSingleConnector(socketAddress, destination, username, password);

    try {
      connector.connect();
      connector.subscribe();
      handleConnector(connector);
    } catch (Exception e) {
      logger.error("process error:", e);
      connector.rollback();
    } finally {
      connector.disconnect();
    }
  }

  private void handleConnector(CanalConnector connector) {
    int batchSize = 1024;
    int maxNoMessageTimes = 60;
    int noMessageTimes = 0;

    while (true) {
      Message message = connector.getWithoutAck(batchSize);
      long batchId = message.getId();
      int size = message.getEntries().size();

      if (batchId == -1 || size == 0) {
        logger.info("no messages.");
        noMessageTimes++;

        if (noMessageTimes >= maxNoMessageTimes) {
          break;
        }

        try {
          TimeUnit.SECONDS.sleep(1);
          continue;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      noMessageTimes = 0;
      printSummary(message, batchId, size);
      printEntries(message.getEntries());
      connector.ack(batchId);
    }
  }

  private void printSummary(Message message, long batchId, int size) {
    long memSize = message.getEntries().stream()
            .mapToLong(entry -> entry.getHeader().getEventLength())
            .sum();
    String startPosition = buildPosition(message.getEntries().get(0));
    String endPosition = buildPosition(message.getEntries().get(size - 1));
    logger.info(context_format, batchId, size, memSize, new SimpleDateFormat(DATE_FORMAT).format(new Date()),
            startPosition, endPosition);
  }

  private String buildPosition(CanalEntry.Entry entry) {
    String position = String.format("%s:%s %s(%s)", entry.getHeader().getLogfileName(),
            entry.getHeader().getLogfileOffset(), entry.getHeader().getExecuteTime(),
            new SimpleDateFormat(DATE_FORMAT).format(new Date(entry.getHeader().getExecuteTime())));

    if (StringUtils.isNotEmpty(entry.getHeader().getGtid())) {
      position = String.format("%s gtid(%s)", position, entry.getHeader().getGtid());
    }

    return position;
  }

  private void printEntries(List<CanalEntry.Entry> entries) {
    entries.forEach(this::printEntry);
  }

  private void printEntry(CanalEntry.Entry entry) {
    if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
      try {
        CanalEntry.TransactionBegin begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
        logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
        printXAInfo(begin.getPropsList());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("parse event has an error, data:" + entry, e);
      }
    } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
      try {
        CanalEntry.TransactionEnd end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
        logger.info(" END ----> transaction id: {} " + LINE_SEPARATOR, end.getTransactionId());
        printXAInfo(end.getPropsList());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("parse event has an error, data:" + entry, e);
      }
    } else if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
      try {
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        CanalEntry.EventType eventType = rowChange.getEventType();
        if (eventType == CanalEntry.EventType.QUERY) {
          logger.info("SQL ----> " + rowChange.getSql());
          return;
        }

        if (rowChange.getIsDdl()) {
          logger.info("DDL: " + true);
          return;
        }

        printXAInfo(rowChange.getPropsList());

        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
          logger.info("Event Type ----> " + rowChange.getEventType());

          if (eventType == CanalEntry.EventType.DELETE) {
            printColumns(rowData.getBeforeColumnsList());
          } else if (eventType == CanalEntry.EventType.INSERT) {
            printColumns(rowData.getAfterColumnsList());
          } else {
            printColumns(rowData.getBeforeColumnsList());
            printColumns(rowData.getAfterColumnsList());
          }
        }
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("parse event has an error, data:" + entry, e);
      }
    }
  }

  private void printXAInfo(List<CanalEntry.Pair> pairs) {
    if (CollectionUtils.isEmpty(pairs)) {
      return;
    }

    String xaType = null;
    String xaXid = null;
    for (CanalEntry.Pair pair : pairs) {
      String key = pair.getKey();
      if (StringUtils.endsWithIgnoreCase(key, "XA_TYPE")) {
        xaType = pair.getValue();
      } else if (StringUtils.endsWithIgnoreCase(key, "XA_XID")) {
        xaXid = pair.getValue();
      }
    }

    if (xaType != null && xaXid != null) {
      logger.info("XA INFO ------> " + xaType + " " + xaXid);
    }
  }

  private void printColumns(List<CanalEntry.Column> columns) {
    for (CanalEntry.Column column : columns) {
      StringBuilder builder = new StringBuilder();

      if (StringUtils.containsIgnoreCase(column.getMysqlType(), "BLOB")
              || StringUtils.containsIgnoreCase(column.getMysqlType(), "BINARY")) {
        builder.append(column.getName()).append(": ")
                .append(new String(column.getValue().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
      } else {
        builder.append(column.getName()).append(": ").append(column.getValue());
      }

      builder.append("  type=").append(column.getMysqlType());

      if (column.getUpdated()) {
        builder.append("  update=").append(column.getUpdated());
      }

      logger.info(builder.toString());
    }
  }

}
