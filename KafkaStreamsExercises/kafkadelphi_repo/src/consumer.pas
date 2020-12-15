unit consumer;

interface

procedure TestKafkaConsumer;

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

procedure msg_consume(rkmessage: Prd_kafka_message_t; opaque: c_pvoid);
var
  msg: string;
begin
  // If you know that bytes are UTF-8, use this line:
//  msg := TEncoding.UTF8.GetString(rkmessage.payload, 0, rkmessage.len);
  msg := TEncoding.ANSI.GetString(rkmessage.payload, 0, rkmessage.len);
  Writeln(Format('msg_consume() -> %s', [msg]));
end;

procedure TestKafkaConsumer;
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
  rkmessage: Prd_kafka_message_t;
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
  rk := rd_kafka_new(RD_KAFKA_CONSUMER, nil, PAnsiChar(buffer[0]), BUFFER_SIZE);
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



  if rd_kafka_consume_start(rkt, partition, 0) = -1 then
  begin
    Writeln(Format('Failed to start consuming: %s', [rd_kafka_err2str(rd_kafka_last_error)]));
    Exit;
  end;

  while True do
  begin
    rd_kafka_poll(rk, 0);

    (* Consume single message.
     * See rdkafka_performance.c for high speed
     * consuming of messages. *)
    rkmessage := rd_kafka_consume(rkt, partition, 1000);
		if rkmessage = nil then
      continue;

		msg_consume(rkmessage, nil);

  	rd_kafka_message_destroy(rkmessage);
  end;

  rd_kafka_consume_stop(rkt, partition);

  while rd_kafka_outq_len(rk) > 0 do
    rd_kafka_poll(rk, 10);

  rd_kafka_topic_destroy(rkt);

  rd_kafka_destroy(rk);
end;

end.
