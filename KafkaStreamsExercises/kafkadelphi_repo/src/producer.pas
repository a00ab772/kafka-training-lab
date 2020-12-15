unit producer;

interface

procedure TestKafkaProducer;

implementation

uses
  System.SysUtils,
  rdkafka;

procedure log_cb(const rk: Prd_kafka_t; level: c_int; const fac, buf: c_pchar); cdecl;
begin
  Writeln('log_cb() called!');
end;

procedure dr_cb(rk: Prd_kafka_t; payload: c_pvoid; len: c_size_t; err: rd_kafka_resp_err_t; opaque, msg_opaque: c_pvoid); cdecl;
begin
  Writeln('dr_cb() called!');
end;

function connect_cb(sockfd: c_int; const addr: c_psockaddr; addrlen: c_int; const id: c_pchar; opaque: c_pvoid): c_int; cdecl;
begin
  Writeln('connect_cb() called!');
end;

procedure TestKafkaProducer;
const
  BUFFER_SIZE: c_size_t = 256;
var
  conf: Prd_kafka_conf_t;
  topic_conf: Prd_kafka_topic_conf_t;
  rk: Prd_kafka_t;
  buffer: TBytes;
  rkt: Prd_kafka_topic_t;

  brokers, topic: c_pchar;
  partition: c_int32_t;

  msg: TBytes;
begin
  brokers := 'localhost';
  topic := 'agustest';
  partition := 0;

  conf := rd_kafka_conf_new;

  rd_kafka_conf_set_log_cb(conf, log_cb);

  topic_conf := rd_kafka_topic_conf_new;

  rd_kafka_conf_set_dr_cb(conf, dr_cb);

  rd_kafka_conf_set_connect_cb(conf, connect_cb);

  SetLength(buffer, BUFFER_SIZE);
  rk := rd_kafka_new(RD_KAFKA_PRODUCER, nil, PAnsiChar(buffer[0]), BUFFER_SIZE);
  if rk = nil then
  begin
    TEncoding.ANSI.GetString(buffer);
    Writeln(Format('ERROR: rd_kafka_new: %s', [buffer]));
    Exit;
  end;

  if rd_kafka_brokers_add(rk, brokers) = 0 then
  begin
    Writeln('No valid brokers specified');
    Exit;
  end;

  rkt := rd_kafka_topic_new(rk, topic, topic_conf);
  topic_conf := nil; // Now owned by topic


  msg := TEncoding.Default.GetBytes('Hello Kafka from Delphi!');
  if rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY, msg, Length(msg), nil, 0, nil) = -1 then
  begin
    Writeln(Format('Failed to produce to topic %s partition %i: %s',
                   [rd_kafka_topic_name(rkt), partition, rd_kafka_err2str(rd_kafka_last_error)]));
    rd_kafka_poll(rk, 0);
    Exit;
  end;
  rd_kafka_poll(rk, 0);
	while (rd_kafka_outq_len(rk) > 0) or True do
    rd_kafka_poll(rk, 100);

  rd_kafka_topic_destroy(rkt);

  rd_kafka_destroy(rk);
end;

end.
