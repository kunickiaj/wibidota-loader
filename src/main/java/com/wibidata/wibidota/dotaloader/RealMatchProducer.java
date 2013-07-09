package com.wibidata.wibidota.dotaloader;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

public class RealMatchProducer extends KijiProducer {
  @Override
  public KijiDataRequest getDataRequest() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public String getOutputColumn() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void produce(KijiRowData kijiRowData, ProducerContext producerContext) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }


}
