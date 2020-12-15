program DKafka;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  rdkafka in 'rdkafka.pas',
  producer in 'producer.pas',
  consumer in 'consumer.pas';

begin
  try
    Writeln(Format('rd_kafka_version(): %x', [rd_kafka_version]));
    Writeln(Format('rd_kafka_version_str(): %s', [rd_kafka_version_str]));
//    TestKafkaProducer;
    TestKafkaConsumer;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
