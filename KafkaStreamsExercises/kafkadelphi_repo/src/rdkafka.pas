unit rdkafka;

interface

// Define macro 'KAFKA_DELPHI_USE_PATCHED_LIBRDKAFKA' if 'librdkafka' has been
// patched and compiled in order to support these functions:
//   * rd_kafka_conf_properties_show2
//   * rd_kafka_dump2
{.$DEFINE KAFKA_DELPHI_USE_PATCHED_LIBRDKAFKA}

uses
{$IF defined(MSWINDOWS)}
  Winapi.Winsock2;
{$ELSE}
  Posix.SysSocket;
{$ENDIF}

const
{$IF defined(MSWINDOWS)}
  LIB_NAME = 'librdkafka.dll';
{$ELSEIF defined(MACOS)}
  LIB_NAME = 'librdkafka.dylib';
{$ELSE}
  LIB_NAME = 'librdkafka.a';
{$ENDIF}

type
  // TODO: Verify type sizes if ported to other platform!
  c_int = Integer;         // int
  c_pint = ^c_int;         // int *
  c_pchar = PAnsiChar;     // char *
  c_ppchar = PPAnsiChar;   // char **
{$IF defined(MSWINDOWS)}
  c_size_t = Cardinal;     // size_t
  c_ssize_t = Integer;     // ssize_t
{$ELSE}
  // Types 'size_t' and 'ssize_t' are 64-bit integer in Linux64
  c_size_t = Int64;        // size_t
  c_ssize_t = UInt64;      // ssize_t
{$ENDIF}
  c_psize_t = ^c_size_t;   // size_t *
  c_int32_t = Int32;       // int32_t
  c_pint32_t = ^c_int32_t; // int32_t *
  c_int64_t = Int64;       // int64_t
  c_pint64_t = ^c_int64_t; // int64_t *
  c_pvoid = Pointer;       // void *
  c_psockaddr = Psockaddr; // sockaddr *
{$IF not defined(KAFKA_DELPHI_USE_PATCHED_LIBRDKAFKA)}
  c_pfile = Pointer;       // FILE *
{$ENDIF}

(*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2013 Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *)

(**
 * @file rdkafka.h
 * @brief Apache Kafka C/C++ consumer and producer client library.
 *
 * rdkafka.h contains the public API for librdkafka.
 * The API is documented in this file as comments prefixing the function, type,
 * enum, define, etc.
 *
 * @sa For the C++ interface see rdkafkacpp.h
 *
 * @tableofcontents
 *)





(**
 * @name librdkafka version
 * @{
 *
 *
 *)

(**
 * @brief librdkafka version
 *
 * Interpreted as hex \c MM.mm.rr.xx:
 *  - MM = Major
 *  - mm = minor
 *  - rr = revision
 *  - xx = pre-release id (0xff is the final release)
 *
 * E.g.: \c 0x000801ff = 0.8.1
 *
 * @remark This value should only be used during compile time,
 *         for runtime checks of version use rd_kafka_version()
 *)
const
  RD_KAFKA_VERSION_: c_int32_t = $000904ff;


(**
 * @brief Returns the librdkafka version as integer.
 *
 * @returns Version integer.
 *
 * @sa See RD_KAFKA_VERSION for how to parse the integer format.
 * @sa Use rd_kafka_version_str() to retreive the version as a string.
 *)
function rd_kafka_version: c_int; cdecl; external LIB_NAME;

(**
 * @brief Returns the librdkafka version as string.
 *
 * @returns Version string
 *)
function rd_kafka_version_str: {const} c_pchar; cdecl; external LIB_NAME;

(**@}*)


(**
 * @name Constants, errors, types
 * @{
 *
 *
 *)


(**
 * @enum rd_kafka_type_t
 *
 * @brief rd_kafka_t handle type.
 *
 * @sa rd_kafka_new()
 *)
type
{$Z4}
  rd_kafka_type_t = (
    RD_KAFKA_PRODUCER, (**< Producer client *)
    RD_KAFKA_CONSUMER  (**< Consumer client *)
  );
{$Z1}


(**
 * @enum Timestamp types
 *
 * @sa rd_kafka_message_timestamp()
 *)
type
{$Z4}
  rd_kafka_timestamp_type_t = (
    RD_KAFKA_TIMESTAMP_NOT_AVAILABLE,  (**< Timestamp not available *)
    RD_KAFKA_TIMESTAMP_CREATE_TIME,    (**< Message creation time *)
    RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME (**< Log append time *)
  );
  Prd_kafka_timestamp_type_t = ^rd_kafka_timestamp_type_t;
{$Z1}



(**
 * @brief Retrieve supported debug contexts for use with the \c \"debug\"
 *        configuration property. (runtime)
 *
 * @returns Comma-separated list of available debugging contexts.
 *)
function rd_kafka_get_debug_contexts: {const} c_pchar; cdecl; external LIB_NAME;

(**
 * @brief Supported debug contexts. (compile time)
 *
 * @deprecated This compile time value may be outdated at runtime due to
 *             linking another version of the library.
 *             Use rd_kafka_get_debug_contexts() instead.
 *)
const
  RD_KAFKA_DEBUG_CONTEXTS: string = 'all,generic,broker,topic,metadata,queue,msg,protocol,cgrp,security,fetch,feature';


(* @cond NO_DOC *)
(* Private types to provide ABI compatibility *)
type
  // typedef struct rd_kafka_s rd_kafka_t;
  rd_kafka_s = record
  end;
  rd_kafka_t = rd_kafka_s;
  Prd_kafka_t = ^rd_kafka_t;
  // typedef struct rd_kafka_topic_s rd_kafka_topic_t;
  rd_kafka_topic_s = record
  end;
  rd_kafka_topic_t = rd_kafka_topic_s;
  Prd_kafka_topic_t = ^rd_kafka_topic_t;
  // typedef struct rd_kafka_conf_s rd_kafka_conf_t;
  rd_kafka_conf_s = record
  end;
  rd_kafka_conf_t = rd_kafka_conf_s;
  Prd_kafka_conf_t = ^rd_kafka_conf_t;
  // typedef struct rd_kafka_topic_conf_s rd_kafka_topic_conf_t;
  rd_kafka_topic_conf_s = record
  end;
  rd_kafka_topic_conf_t = rd_kafka_topic_conf_s;
  Prd_kafka_topic_conf_t = ^rd_kafka_topic_conf_t;
  // typedef struct rd_kafka_queue_s rd_kafka_queue_t;
  rd_kafka_queue_s = record
  end;
  rd_kafka_queue_t = rd_kafka_queue_s;
  Prd_kafka_queue_t = ^rd_kafka_queue_t;
(* @endcond *)


(**
 * @enum rd_kafka_resp_err_t
 * @brief Error codes.
 *
 * The negative error codes delimited by two underscores
 * (\c RD_KAFKA_RESP_ERR__..) denotes errors internal to librdkafka and are
 * displayed as \c \"Local: \<error string..\>\", while the error codes
 * delimited by a single underscore (\c RD_KAFKA_RESP_ERR_..) denote broker
 * errors and are displayed as \c \"Broker: \<error string..\>\".
 *
 * @sa Use rd_kafka_err2str() to translate an error code a human readable string
 *)
type
{$Z4}
  rd_kafka_resp_err_t = (
    (* Internal errors to rdkafka: *)
    (** Begin internal error codes *)
    RD_KAFKA_RESP_ERR__BEGIN = -200,
    (** Received message is incorrect *)
    RD_KAFKA_RESP_ERR__BAD_MSG = -199,
    (** Bad/unknown compression *)
    RD_KAFKA_RESP_ERR__BAD_COMPRESSION = -198,
    (** Broker is going away *)
    RD_KAFKA_RESP_ERR__DESTROY = -197,
    (** Generic failure *)
    RD_KAFKA_RESP_ERR__FAIL = -196,
    (** Broker transport failure *)
    RD_KAFKA_RESP_ERR__TRANSPORT = -195,
    (** Critical system resource *)
    RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE = -194,
    (** Failed to resolve broker *)
    RD_KAFKA_RESP_ERR__RESOLVE = -193,
    (** Produced message timed out*)
    RD_KAFKA_RESP_ERR__MSG_TIMED_OUT = -192,
    (** Reached the end of the topic+partition queue on
    * the broker. Not really an error. *)
    RD_KAFKA_RESP_ERR__PARTITION_EOF = -191,
    (** Permanent: Partition does not exist in cluster. *)
    RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION = -190,
    (** File or filesystem error *)
    RD_KAFKA_RESP_ERR__FS = -189,
    (** Permanent: Topic does not exist in cluster. *)
    RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC = -188,
    (** All broker connections are down. *)
    RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN = -187,
    (** Invalid argument, or invalid configuration *)
    RD_KAFKA_RESP_ERR__INVALID_ARG = -186,
    (** Operation timed out *)
    RD_KAFKA_RESP_ERR__TIMED_OUT = -185,
    (** Queue is full *)
    RD_KAFKA_RESP_ERR__QUEUE_FULL = -184,
    (** ISR count < required.acks *)
    RD_KAFKA_RESP_ERR__ISR_INSUFF = -183,
    (** Broker node update *)
    RD_KAFKA_RESP_ERR__NODE_UPDATE = -182,
    (** SSL error *)
    RD_KAFKA_RESP_ERR__SSL = -181,
    (** Waiting for coordinator to become available. *)
    RD_KAFKA_RESP_ERR__WAIT_COORD = -180,
    (** Unknown client group *)
    RD_KAFKA_RESP_ERR__UNKNOWN_GROUP = -179,
    (** Operation in progress *)
    RD_KAFKA_RESP_ERR__IN_PROGRESS = -178,
    (** Previous operation in progress, wait for it to finish. *)
    RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS = -177,
    (** This operation would interfere with an existing subscription *)
    RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION = -176,
    (** Assigned partitions (rebalance_cb) *)
    RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS = -175,
    (** Revoked partitions (rebalance_cb) *)
    RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS = -174,
    (** Conflicting use *)
    RD_KAFKA_RESP_ERR__CONFLICT = -173,
    (** Wrong state *)
    RD_KAFKA_RESP_ERR__STATE = -172,
    (** Unknown protocol *)
    RD_KAFKA_RESP_ERR__UNKNOWN_PROTOCOL = -171,
    (** Not implemented *)
    RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED = -170,
    (** Authentication failure*)
    RD_KAFKA_RESP_ERR__AUTHENTICATION = -169,
    (** No stored offset *)
    RD_KAFKA_RESP_ERR__NO_OFFSET = -168,
    (** Outdated *)
    RD_KAFKA_RESP_ERR__OUTDATED = -167,
    (** Timed out in queue *)
    RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE = -166,
    (** Feature not supported by broker *)
    RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE = -165,
    (** Awaiting cache update *)
    RD_KAFKA_RESP_ERR__WAIT_CACHE = -164,

    (** End internal error codes *)
    RD_KAFKA_RESP_ERR__END = -100,

    (* Kafka broker errors: *)
    (** Unknown broker error *)
    RD_KAFKA_RESP_ERR_UNKNOWN = -1,
    (** Success *)
    RD_KAFKA_RESP_ERR_NO_ERROR = 0,
    (** Offset out of range *)
    RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE = 1,
    (** Invalid message *)
    RD_KAFKA_RESP_ERR_INVALID_MSG = 2,
    (** Unknown topic or partition *)
    RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART = 3,
    (** Invalid message size *)
    RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE = 4,
    (** Leader not available *)
    RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE = 5,
    (** Not leader for partition *)
    RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION = 6,
    (** Request timed out *)
    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT = 7,
    (** Broker not available *)
    RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE = 8,
    (** Replica not available *)
    RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE = 9,
    (** Message size too large *)
    RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE = 10,
    (** StaleControllerEpochCode *)
    RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH = 11,
    (** Offset metadata string too large *)
    RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE = 12,
    (** Broker disconnected before response received *)
    RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION = 13,
    (** Group coordinator load in progress *)
    RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS = 14,
    (** Group coordinator not available *)
    RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE = 15,
    (** Not coordinator for group *)
    RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP = 16,
    (** Invalid topic *)
    RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION = 17,
    (** Message batch larger than configured server segment size *)
    RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE = 18,
    (** Not enough in-sync replicas *)
    RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS = 19,
    (** Message(s) written to insufficient number of in-sync replicas *)
    RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
    (** Invalid required acks value *)
    RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS = 21,
    (** Specified group generation id is not valid *)
    RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION = 22,
    (** Inconsistent group protocol *)
    RD_KAFKA_RESP_ERR_INCONSISTENT_GROUP_PROTOCOL = 23,
    (** Invalid group.id *)
    RD_KAFKA_RESP_ERR_INVALID_GROUP_ID = 24,
    (** Unknown member *)
    RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID = 25,
    (** Invalid session timeout *)
    RD_KAFKA_RESP_ERR_INVALID_SESSION_TIMEOUT = 26,
    (** Group rebalance in progress *)
    RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS = 27,
    (** Commit offset data size is not valid *)
    RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE = 28,
    (** Topic authorization failed *)
    RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED = 29,
    (** Group authorization failed *)
    RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED = 30,
    (** Cluster authorization failed *)
    RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED = 31,
    (** Invalid timestamp *)
    RD_KAFKA_RESP_ERR_INVALID_TIMESTAMP = 32,
    (** Unsupported SASL mechanism *)
    RD_KAFKA_RESP_ERR_UNSUPPORTED_SASL_MECHANISM = 33,
    (** Illegal SASL state *)
    RD_KAFKA_RESP_ERR_ILLEGAL_SASL_STATE = 34,
    (** Unuspported version *)
    RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION = 35,
    (** Topic already exists *)
    RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS = 36,
    (** Invalid number of partitions *)
    RD_KAFKA_RESP_ERR_INVALID_PARTITIONS = 37,
    (** Invalid replication factor *)
    RD_KAFKA_RESP_ERR_INVALID_REPLICATION_FACTOR = 38,
    (** Invalid replica assignment *)
    RD_KAFKA_RESP_ERR_INVALID_REPLICA_ASSIGNMENT = 39,
    (** Invalid config *)
    RD_KAFKA_RESP_ERR_INVALID_CONFIG = 40,
    (** Not controller for cluster *)
    RD_KAFKA_RESP_ERR_NOT_CONTROLLER = 41,
    (** Invalid request *)
    RD_KAFKA_RESP_ERR_INVALID_REQUEST = 42,
    (** Message format on broker does not support request *)
    RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT = 43,

    RD_KAFKA_RESP_ERR_END_ALL
  );
{$Z1}


(**
 * @brief Error code value, name and description.
 *        Typically for use with language bindings to automatically expose
 *        the full set of librdkafka error codes.
 *)
type
  rd_kafka_err_desc = record
    code: rd_kafka_resp_err_t; (**< Error code *)
    {const} name: c_pchar;     (**< Error name, same as code enum sans prefix *)
    {const} desc: c_pchar;     (**< Human readable error description. *)
  end;
  Prd_kafka_err_desc = ^rd_kafka_err_desc;
  PPrd_kafka_err_desc = ^Prd_kafka_err_desc;


(**
 * @brief Returns the full list of error codes.
 *)
procedure rd_kafka_get_err_descs(const errdescs: PPrd_kafka_err_desc; cntp: c_psize_t); cdecl; external LIB_NAME;




(**
 * @brief Returns a human readable representation of a kafka error.
 *
 * @param err Error code to translate
 *)
function rd_kafka_err2str(err: rd_kafka_resp_err_t): {const} c_pchar; cdecl; external LIB_NAME;



(**
 * @brief Returns the error code name (enum name).
 *
 * @param err Error code to translate
 *)
function rd_kafka_err2name(err: rd_kafka_resp_err_t ): {const} c_pchar; cdecl; external LIB_NAME;


(**
 * @brief Returns the last error code generated by a legacy API call
 *        in the current thread.
 *
 * The legacy APIs are the ones using errno to propagate error value, namely:
 *  - rd_kafka_topic_new()
 *  - rd_kafka_consume_start()
 *  - rd_kafka_consume_stop()
 *  - rd_kafka_consume()
 *  - rd_kafka_consume_batch()
 *  - rd_kafka_consume_callback()
 *  - rd_kafka_consume_queue()
 *  - rd_kafka_produce()
 *
 * The main use for this function is to avoid converting system \p errno
 * values to rd_kafka_resp_err_t codes for legacy APIs.
 *
 * @remark The last error is stored per-thread, if multiple rd_kafka_t handles
 *         are used in the same application thread the developer needs to
 *         make sure rd_kafka_last_error() is called immediately after
 *         a failed API call.
 *)
function rd_kafka_last_error: rd_kafka_resp_err_t ; cdecl; external LIB_NAME;


(**
 * @brief Converts the system errno value \p errnox to a rd_kafka_resp_err_t
 *        error code upon failure from the following functions:
 *  - rd_kafka_topic_new()
 *  - rd_kafka_consume_start()
 *  - rd_kafka_consume_stop()
 *  - rd_kafka_consume()
 *  - rd_kafka_consume_batch()
 *  - rd_kafka_consume_callback()
 *  - rd_kafka_consume_queue()
 *  - rd_kafka_produce()
 *
 * @param errnox  System errno value to convert
 *
 * @returns Appropriate error code for \p errnox
 *
 * @remark A better alternative is to call rd_kafka_last_error() immediately
 *         after any of the above functions return -1 or NULL.
 *
 * @sa rd_kafka_last_error()
 *)
function rd_kafka_errno2err(errnox: c_int): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**
 * @brief Returns the thread-local system errno
 *
 * On most platforms this is the same as \p errno but in case of different
 * runtimes between library and application (e.g., Windows static DLLs)
 * this provides a means for expsing the errno librdkafka uses.
 *
 * @remark The value is local to the current calling thread.
 *)
function rd_kafka_errno: c_int; cdecl; external LIB_NAME;



(**
 * @brief Topic+Partition place holder
 *
 * Generic place holder for a Topic+Partition and its related information
 * used for multiple purposes:
 *   - consumer offset (see rd_kafka_commit(), et.al.)
 *   - group rebalancing callback (rd_kafka_conf_set_rebalance_cb())
 *   - offset commit result callback (rd_kafka_conf_set_offset_commit_cb())
 *)

(**
 * @brief Generic place holder for a specific Topic+Partition.
 *
 * @sa rd_kafka_topic_partition_list_new()
 *)
type
  // C tag: rd_kafka_topic_partition_s
  rd_kafka_topic_partition_t  = record
    topic: c_pchar;           (**< Topic name *)
    partition: c_int32_t;     (**< Partition *)
    offset: c_int64_t;        (**< Offset *)
    metadata: c_pvoid;        (**< Metadata *)
    metadata_size: c_size_t;  (**< Metadata size *)
    opaque: c_pvoid;          (**< Application opaque *)
    err: rd_kafka_resp_err_t; (**< Error code, depending on use. *)
    _private: c_pvoid;        (**< INTERNAL USE ONLY,
                               *   INITIALIZE TO ZERO, DO NOT TOUCH *)
  end;
  Prd_kafka_topic_partition_t = ^rd_kafka_topic_partition_t;


(**
 * @brief Destroy a rd_kafka_topic_partition_t.
 * @remark This must not be called for elements in a topic partition list.
 *)
procedure rd_kafka_topic_partition_destroy(rktpar: Prd_kafka_topic_partition_t); cdecl; external LIB_NAME;


(**
 * @brief A growable list of Topic+Partitions.
 *
 *)
type
  // C tag: rd_kafka_topic_partition_list_s
  rd_kafka_topic_partition_list_t = record
    cnt: c_int;                         (**< Current number of elements *)
    size: c_int;                        (**< Current allocated size *)
    elems: Prd_kafka_topic_partition_t; (**< Element array[] *)
  end;
  Prd_kafka_topic_partition_list_t = ^rd_kafka_topic_partition_list_t;
  PPrd_kafka_topic_partition_list_t = ^Prd_kafka_topic_partition_list_t;


(**
 * @brief Create a new list/vector Topic+Partition container.
 *
 * @param size  Initial allocated size used when the expected number of
 *              elements is known or can be estimated.
 *              Avoids reallocation and possibly relocation of the
 *              elems array.
 *
 * @returns A newly allocated Topic+Partition list.
 *
 * @remark Use rd_kafka_topic_partition_list_destroy() to free all resources
 *         in use by a list and the list itself.
 * @sa     rd_kafka_topic_partition_list_add()
 *)
function rd_kafka_topic_partition_list_new(size: c_int): Prd_kafka_topic_partition_list_t; cdecl; external LIB_NAME;


(**
 * @brief Free all resources used by the list and the list itself.
 *)
procedure rd_kafka_topic_partition_list_destroy(rkparlist: Prd_kafka_topic_partition_list_t); cdecl; external LIB_NAME;

(**
 * @brief Add topic+partition to list
 *
 * @param rktparlist List to extend
 * @param topic      Topic name (copied)
 * @param partition  Partition id
 *
 * @returns The object which can be used to fill in additionals fields.
 *)
function rd_kafka_topic_partition_list_add(rktparlist: Prd_kafka_topic_partition_list_t; const topic: c_pchar;
  partition: c_int32_t): Prd_kafka_topic_partition_list_t; cdecl; external LIB_NAME;


(**
 * @brief Add range of partitions from \p start to \p stop inclusive.
 *
 * @param rktparlist List to extend
 * @param topic      Topic name (copied)
 * @param start      Start partition of range
 * @param stop       Last partition of range (inclusive)
 *)
procedure rd_kafka_topic_partition_list_add_range(rktparlist: Prd_kafka_topic_partition_list_t; const topic: c_pchar;
  start, stop: c_int32_t) cdecl; external LIB_NAME;



(**
 * @brief Delete partition from list.
 *
 * @param rktparlist List to modify
 * @param topic      Topic name to match
 * @param partition  Partition to match
 *
 * @returns 1 if partition was found (and removed), else 0.
 *
 * @remark Any held indices to elems[] are unusable after this call returns 1.
 *)
function rd_kafka_topic_partition_list_del(rktparlist: Prd_kafka_topic_partition_list_t; const topic: c_pchar;
  partition: c_int32_t): c_int; cdecl; external LIB_NAME;


(**
 * @brief Delete partition from list by elems[] index.
 *
 * @returns 1 if partition was found (and removed), else 0.
 *
 * @sa rd_kafka_topic_partition_list_del()
 *)
function rd_kafka_topic_partition_list_del_by_idx(rktparlist: Prd_kafka_topic_partition_list_t; idx: c_int): c_int; cdecl;
  external LIB_NAME;


(**
 * @brief Make a copy of an existing list.
 *
 * @param src   The existing list to copy.
 *
 * @returns A new list fully populated to be identical to \p src
 *)
function rd_kafka_topic_partition_list_copy(const src: Prd_kafka_topic_partition_list_t): Prd_kafka_topic_partition_list_t; cdecl;
  external LIB_NAME;




(**
 * @brief Set offset to \p offset for \p topic and \p partition
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success or
 *          RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION if \p partition was not found
 *          in the list.
 *)
function rd_kafka_topic_partition_list_set_offset(rktparlist: Prd_kafka_topic_partition_list_t; const topic: c_pchar; partition: c_int32_t;
  offset: c_int64_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;



(**
 * @brief Find element by \p topic and \p partition.
 *
 * @returns a pointer to the first matching element, or NULL if not found.
 *)
function rd_kafka_topic_partition_list_find(rktparlist: Prd_kafka_topic_partition_list_t; const topic: c_pchar;
  partition: c_int32_t): Prd_kafka_topic_partition_t; cdecl; external LIB_NAME;


(**
 * @brief Sort list using comparator \p cmp.
 *
 * If \p cmp is NULL the default comparator will be used that
 * sorts by ascending topic name and partition.
 *
 *)
type
  rd_kafka_topic_partition_list_sort_fn = function(const a, b: c_pvoid; opaque: c_pvoid): c_int; cdecl;

procedure rd_kafka_topic_partition_list_sort(rktparlist: Prd_kafka_topic_partition_list_t; cmp: rd_kafka_topic_partition_list_sort_fn;
  opaque: c_pvoid); cdecl; external LIB_NAME;


(**@}*)



(**
 * @name Var-arg tag types
 * @{
 *
 *)

(**
 * @enum rd_kafka_vtype_t
 *
 * @brief Var-arg tag types
 *
 * @sa rd_kafka_producev()
 *)
type
{$Z4}
  rd_kafka_vtype_t = (
    RD_KAFKA_VTYPE_END,       (**< va-arg sentinel *)
    RD_KAFKA_VTYPE_TOPIC,     (**< (const c_pchar) Topic name *)
    RD_KAFKA_VTYPE_RKT,       (**< (Prd_kafka_topic_t) Topic handle *)
    RD_KAFKA_VTYPE_PARTITION, (**< (c_int32_t) Partition *)
    RD_KAFKA_VTYPE_VALUE,     (**< (c_pvoid, c_size_t) Message value (payload)*)
    RD_KAFKA_VTYPE_KEY,       (**< (c_pvoid, c_size_t) Message key *)
    RD_KAFKA_VTYPE_OPAQUE,    (**< (c_pvoid) Application opaque *)
    RD_KAFKA_VTYPE_MSGFLAGS,  (**< (c_int) RD_KAFKA_MSG_F_.. flags *)
    RD_KAFKA_VTYPE_TIMESTAMP  (**< (c_int64_t) Milliseconds since epoch UTC *)
  );
{$Z1}


(*!
 * va-arg end sentinel used to terminate the variable argument list
 *)
const
  RD_KAFKA_V_END: c_int32_t = Ord(RD_KAFKA_VTYPE_END);


(**
 * @name Kafka messages
 * @{
 *
 *)



// FIXME: This doesn't show up in docs for some reason
// "Compound rd_kafka_message_t is not documented."

(**
 * @brief A Kafka message as returned by the \c rd_kafka_consume*() family
 *        of functions as well as provided to the Producer \c dr_msg_cb().
 *
 * For the consumer this object has two purposes:
 *  - provide the application with a consumed message. (\c err == 0)
 *  - report per-topic+partition consumer errors (\c err != 0)
 *
 * The application must check \c err to decide what action to take.
 *
 * When the application is finished with a message it must call
 * rd_kafka_message_destroy() unless otherwise noted.
 *)
type
  // C tag: rd_kafka_message_s
  rd_kafka_message_t = record
    err: rd_kafka_resp_err_t; (**< Non-zero for error signaling. *)
    rkt: Prd_kafka_topic_t;   (**< Topic *)
    partition: c_int32_t;     (**< Partition *)
    payload: c_pvoid;         (**< Producer: original message payload.
                               * Consumer: Depends on the value of \c err :
                               * - \c err==0: Message payload.
                               * - \c err!=0: Error string *)
    len: c_size_t;            (**< Depends on the value of \c err :
                               * - \c err==0: Message payload length
                               * - \c err!=0: Error string length *)
    key: c_pvoid;             (**< Depends on the value of \c err :
                               * - \c err==0: Optional message key *)
    key_len: c_size_t;        (**< Depends on the value of \c err :
                               * - \c err==0: Optional message key length*)
    offset: c_int64_t;        (**< Consume:
                               * - Message offset (or offset for error
                               *   if \c err!=0 if applicable).
                               * - dr_msg_cb:
                               *   Message offset assigned by broker.
                               *   If \c produce.offset.report is set then
                               *   each message will have this field set,
                               *   otherwise only the last message in
                               *   each produced internal batch will
                               *   have this field set, otherwise 0. *)
    _private: c_pvoid;        (**< Consume:
                               *  - rdkafka private pointer: DO NOT MODIFY
                               *  - dr_msg_cb:
                               *    msg_opaque from produce() call *)
  end;
  Prd_kafka_message_t = ^rd_kafka_message_t;
  PPrd_kafka_message_t = ^Prd_kafka_message_t;


(**
 * @brief Frees resources for \p rkmessage and hands ownership back to rdkafka.
 *)
procedure rd_kafka_message_destroy(rkmessage: Prd_kafka_message_t); cdecl; external LIB_NAME;




(**
 * @brief Returns the error string for an errored rd_kafka_message_t or NULL if
 *        there was no error.
 *)
function rd_kafka_message_errstr(const rkmessage: Prd_kafka_message_t): {const} c_pchar; inline;



(**
 * @brief Returns the message timestamp for a consumed message.
 *
 * The timestamp is the number of milliseconds since the epoch (UTC).
 *
 * \p tstype (if not NULL) is updated to indicate the type of timestamp.
 *
 * @returns message timestamp, or -1 if not available.
 *
 * @remark Message timestamps require broker version 0.10.0 or later.
 *)
function rd_kafka_message_timestamp(const rkmessage: Prd_kafka_message_t; tstype: Prd_kafka_timestamp_type_t): c_int64_t; cdecl;
  external LIB_NAME;



(**@}*)


(**
 * @name Configuration interface
 * @{
 *
 * @brief Main/global configuration property interface
 *
 *)

(**
 * @enum rd_kafka_conf_res_t
 * @brief Configuration result type
 *)
type
{$Z4}
  rd_kafka_conf_res_t = (
    RD_KAFKA_CONF_UNKNOWN = -2, (**< Unknown configuration name. *)
    RD_KAFKA_CONF_INVALID = -1, (**< Invalid configuration value. *)
    RD_KAFKA_CONF_OK = 0        (**< Configuration okay *)
  );
{$Z1}


(**
 * @brief Create configuration object.
 *
 * When providing your own configuration to the \c rd_kafka_*_new_*() calls
 * the rd_kafka_conf_t objects needs to be created with this function
 * which will set up the defaults.
 * I.e.:
 * @code
 *   rd_kafka_conf_t *myconf;
 *   rd_kafka_conf_res_t res;
 *
 *   myconf = rd_kafka_conf_new();
 *   res = rd_kafka_conf_set(myconf, "socket.timeout.ms", "600",
 *                           errstr, sizeof(errstr));
 *   if (res != RD_KAFKA_CONF_OK)
 *      die("%s\n", errstr);
 *
 *   rk = rd_kafka_new(..., myconf);
 * @endcode
 *
 * Please see CONFIGURATION.md for the default settings or use
 * rd_kafka_conf_properties_show() to provide the information at runtime.
 *
 * The properties are identical to the Apache Kafka configuration properties
 * whenever possible.
 *
 * @returns A new rd_kafka_conf_t object with defaults set.
 *
 * @sa rd_kafka_conf_set(), rd_kafka_conf_destroy()
 *)
function rd_kafka_conf_new: Prd_kafka_conf_t; cdecl; external LIB_NAME;


(**
 * @brief Destroys a conf object.
 *)
procedure rd_kafka_conf_destroy(conf: Prd_kafka_conf_t); cdecl; external LIB_NAME;


(**
 * @brief Creates a copy/duplicate of configuration object \p conf
 *)
function rd_kafka_conf_dup(const conf: Prd_kafka_conf_t): Prd_kafka_conf_t; cdecl; external LIB_NAME;


(**
 * @brief Sets a configuration property.
 *
 * \p conf must have been previously created with rd_kafka_conf_new().
 *
 * Returns \c rd_kafka_conf_res_t to indicate success or failure.
 * In case of failure \p errstr is updated to contain a human readable
 * error string.
 *)
function rd_kafka_conf_set(conf: Prd_kafka_conf_t; const name, value: c_pchar; errstr: c_pchar;
  errstr_size: c_size_t): rd_kafka_conf_res_t; cdecl; external LIB_NAME;


(**
 * @brief Enable event sourcing.
 * \p events is a bitmask of \c RD_KAFKA_EVENT_* of events to enable
 * for consumption by `rd_kafka_queue_poll()`.
 *)
procedure rd_kafka_conf_set_events(conf: Prd_kafka_conf_t; events: c_int); cdecl; external LIB_NAME;


(**
 @deprecated See rd_kafka_conf_set_dr_msg_cb()
*)
type
  rd_kafka_conf_set_dr_cb_fn = procedure(rk: Prd_kafka_t; payload: c_pvoid; len: c_size_t; err: rd_kafka_resp_err_t;
                                 opaque, msg_opaque: c_pvoid); cdecl;

procedure rd_kafka_conf_set_dr_cb(conf: Prd_kafka_conf_t; dr_cb: rd_kafka_conf_set_dr_cb_fn); cdecl; external LIB_NAME;

(**
 * @brief \b Producer: Set delivery report callback in provided \p conf object.
 *
 * The delivery report callback will be called once for each message
 * accepted by rd_kafka_produce() (et.al) with \p err set to indicate
 * the result of the produce request.
 *
 * The callback is called when a message is succesfully produced or
 * if librdkafka encountered a permanent failure, or the retry counter for
 * temporary errors has been exhausted.
 *
 * An application must call rd_kafka_poll() at regular intervals to
 * serve queued delivery report callbacks.
 *)
type
  rd_kafka_conf_set_dr_msg_cb_fn = procedure(rk: Prd_kafka_t; const rkmessage: Prd_kafka_message_t; opaque: c_pvoid); cdecl;

procedure rd_kafka_conf_set_dr_msg_cb(conf: Prd_kafka_conf_t; dr_msg_cb: rd_kafka_conf_set_dr_msg_cb_fn); cdecl; external LIB_NAME;


(**
 * @brief \b Consumer: Set consume callback for use with rd_kafka_consumer_poll()
 *
 *)
type
  rd_kafka_conf_set_consume_cb_fn = procedure(rkmessage: Prd_kafka_message_t; opaque: c_pvoid); cdecl;

procedure rd_kafka_conf_set_consume_cb(conf: Prd_kafka_conf_t; consume_cb: rd_kafka_conf_set_consume_cb_fn); cdecl; external LIB_NAME;

(**
 * @brief \b Consumer: Set rebalance callback for use with
 *                     coordinated consumer group balancing.
 *
 * The \p err field is set to either RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
 * or RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS and 'partitions'
 * contains the full partition set that was either assigned or revoked.
 *
 * Registering a \p rebalance_cb turns off librdkafka's automatic
 * partition assignment/revocation and instead delegates that responsibility
 * to the application's \p rebalance_cb.
 *
 * The rebalance callback is responsible for updating librdkafka's
 * assignment set based on the two events: RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
 * and RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS but should also be able to handle
 * arbitrary rebalancing failures where \p err is neither of those.
 * @remark In this latter case (arbitrary error), the application must
 *         call rd_kafka_assign(rk, NULL) to synchronize state.
 *
 * Without a rebalance callback this is done automatically by librdkafka
 * but registering a rebalance callback gives the application flexibility
 * in performing other operations along with the assinging/revocation,
 * such as fetching offsets from an alternate location (on assign)
 * or manually committing offsets (on revoke).
 *
 * @remark The \p partitions list is destroyed by librdkafka on return
 *         return from the rebalance_cb and must not be freed or
 *         saved by the application.
 *
 * The following example shows the application's responsibilities:
 * @code
 *    static void rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
 *                              rd_kafka_topic_partition_list_t *partitions,
 *                              void *opaque) {
 *
 *        switch (err)
 *        {
 *          case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
 *             // application may load offets from arbitrary external
 *             // storage here and update \p partitions
 *
 *             rd_kafka_assign(rk, partitions);
 *             break;
 *
 *          case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
 *             if (manual_commits) // Optional explicit manual commit
 *                 rd_kafka_commit(rk, partitions, 0); // sync commit
 *
 *             rd_kafka_assign(rk, NULL);
 *             break;
 *
 *          default:
 *             handle_unlikely_error(err);
 *             rd_kafka_assign(rk, NULL); // sync state
 *             break;
 *         }
 *    }
 * @endcode
 *)
type
  rd_kafka_conf_set_rebalance_cb_fn = procedure(rk: Prd_kafka_t; err: rd_kafka_resp_err_t; partitions: Prd_kafka_topic_partition_list_t;
                                        opaque: c_pvoid); cdecl;

procedure rd_kafka_conf_set_rebalance_cb(conf: Prd_kafka_conf_t; rebalance_cb: rd_kafka_conf_set_rebalance_cb_fn); cdecl; external LIB_NAME;



(**
 * @brief \b Consumer: Set offset commit callback for use with consumer groups.
 *
 * The results of automatic or manual offset commits will be scheduled
 * for this callback and is served by rd_kafka_consumer_poll().
 *
 * If no partitions had valid offsets to commit this callback will be called
 * with \p err == RD_KAFKA_RESP_ERR__NO_OFFSET which is not to be considered
 * an error.
 *
 * The \p offsets list contains per-partition information:
 *   - \c offset: committed offset (attempted)
 *   - \c err:    commit error
 *)
type
  rd_kafka_conf_set_offset_commit_cb_fn = procedure(rk: Prd_kafka_t; err: rd_kafka_resp_err_t; offsets: Prd_kafka_topic_partition_list_t;
                                            opaque: c_pvoid); cdecl;

procedure rd_kafka_conf_set_offset_commit_cb(conf: Prd_kafka_conf_t; offset_commit_cb: rd_kafka_conf_set_offset_commit_cb_fn); cdecl;
  external LIB_NAME;


(**
 * @brief Set error callback in provided conf object.
 *
 * The error callback is used by librdkafka to signal critical errors
 * back to the application.
 *
 * If no \p error_cb is registered then the errors will be logged instead.
 *)
type
  rd_kafka_conf_set_error_cb_fn = procedure(rk: Prd_kafka_t; err: c_int; const reason: c_pchar; opaque: c_pvoid); cdecl;

procedure rd_kafka_conf_set_error_cb(conf: Prd_kafka_conf_t; error_cb: rd_kafka_conf_set_error_cb_fn); cdecl; external LIB_NAME;

(**
 * @brief Set throttle callback.
 *
 * The throttle callback is used to forward broker throttle times to the
 * application for Produce and Fetch (consume) requests.
 *
 * Callbacks are triggered whenever a non-zero throttle time is returned by
 * the broker, or when the throttle time drops back to zero.
 *
 * An application must call rd_kafka_poll() or rd_kafka_consumer_poll() at
 * regular intervals to serve queued callbacks.
 *
 * @remark Requires broker version 0.9.0 or later.
 *)
type
  rd_kafka_conf_set_throttle_cb_fn = procedure(rk: Prd_kafka_t; const broker_name: c_pchar; broker_id: c_int32_t; throttle_time_ms: c_int;
                                       opaque: c_pvoid); cdecl;

procedure rd_kafka_conf_set_throttle_cb(conf: Prd_kafka_conf_t; throttle_cb: rd_kafka_conf_set_throttle_cb_fn); cdecl; external LIB_NAME;


(**
 * @brief Set logger callback.
 *
 * The default is to print to stderr, but a syslog logger is also available,
 * see rd_kafka_log_print and rd_kafka_log_syslog for the builtin alternatives.
 * Alternatively the application may provide its own logger callback.
 * Or pass \p func as NULL to disable logging.
 *
 * This is the configuration alternative to the deprecated rd_kafka_set_logger()
 *
 * @remark The log_cb will be called spontaneously from librdkafka's internal
 *         threads unless logs have been forwarded to a poll queue through
 *         \c rd_kafka_set_log_queue().
 *         An application MUST NOT call any librdkafka APIs or do any prolonged
 *         work in a non-forwarded \c log_cb.
 *)
type
  rd_kafka_conf_set_log_cb_fn = procedure(const rk: Prd_kafka_t; level: c_int; const fac, buf: c_pchar); cdecl;

procedure rd_kafka_conf_set_log_cb(conf: Prd_kafka_conf_t; log_cb: rd_kafka_conf_set_log_cb_fn); cdecl; external LIB_NAME;


(**
 * @brief Set statistics callback in provided conf object.
 *
 * The statistics callback is triggered from rd_kafka_poll() every
 * \c statistics.interval.ms (needs to be configured separately).
 * Function arguments:
 *   - \p rk - Kafka handle
 *   - \p json - String containing the statistics data in JSON format
 *   - \p json_len - Length of \p json string.
 *   - \p opaque - Application-provided opaque.
 *
 * If the application wishes to hold on to the \p json pointer and free
 * it at a later time it must return 1 from the \p stats_cb.
 * If the application returns 0 from the \p stats_cb then librdkafka
 * will immediately free the \p json pointer.
 *)
type
  rd_kafka_conf_set_stats_cb_fn = function(rk: Prd_kafka_t; json: c_pchar; json_len: c_size_t; opaque: c_pvoid): c_int; cdecl;

procedure rd_kafka_conf_set_stats_cb(conf: Prd_kafka_conf_t; stats_cb: rd_kafka_conf_set_stats_cb_fn); cdecl; external LIB_NAME;



(**
 * @brief Set socket callback.
 *
 * The socket callback is responsible for opening a socket
 * according to the supplied \p domain, \p type and \p protocol.
 * The socket shall be created with \c CLOEXEC set in a racefree fashion, if
 * possible.
 *
 * Default:
 *  - on linux: racefree CLOEXEC
 *  - others  : non-racefree CLOEXEC
 *
 * @remark The callback will be called from an internal librdkafka thread.
 *)
type
  rd_kafka_conf_set_socket_cb_fn = function(domain, _type, protocol: c_int; opaque: c_pvoid): c_int; cdecl;

procedure rd_kafka_conf_set_socket_cb(conf: Prd_kafka_conf_t; socket_cb: rd_kafka_conf_set_socket_cb_fn); cdecl; external LIB_NAME;



(**
 * @brief Set connect callback.
 *
 * The connect callback is responsible for connecting socket \p sockfd
 * to peer address \p addr.
 * The \p id field contains the broker identifier.
 *
 * \p connect_cb shall return 0 on success (socket connected) or an error
 * number (errno) on error.
 *
 * @remark The callback will be called from an internal librdkafka thread.
 *)
type
  rd_kafka_conf_set_connect_cb_fn = function(sockfd: c_int; const addr: c_psockaddr; addrlen: c_int; const id: c_pchar;
                                      opaque: c_pvoid): c_int; cdecl;

procedure rd_kafka_conf_set_connect_cb(conf: Prd_kafka_conf_t; connect_cb: rd_kafka_conf_set_connect_cb_fn); cdecl; external LIB_NAME;

(**
 * @brief Set close socket callback.
 *
 * Close a socket (optionally opened with socket_cb()).
 *
 * @remark The callback will be called from an internal librdkafka thread.
 *)
type
  rd_kafka_conf_set_closesocket_cb_fn = function(sockfd: c_int; opaque: c_pvoid): c_int; cdecl;

procedure rd_kafka_conf_set_closesocket_cb(conf: Prd_kafka_conf_t; closesocket_cb: rd_kafka_conf_set_closesocket_cb_fn); cdecl;
  external LIB_NAME;



{$IF not defined(MSWINDOWS)}
// #ifndef _MSC_VER
(**
 * @brief Set open callback.
 *
 * The open callback is responsible for opening the file specified by
 * pathname, flags and mode.
 * The file shall be opened with \c CLOEXEC set in a racefree fashion, if
 * possible.
 *
 * Default:
 *  - on linux: racefree CLOEXEC
 *  - others  : non-racefree CLOEXEC
 *
 * @remark The callback will be called from an internal librdkafka thread.
 *)
type
  // TODO: Use actual Delphi's 'mode_t' translation
  mode_t = UInt16;
  rd_kafka_conf_set_open_cb_fn = function(const pathname: c_pchar; flags: c_int; mode: mode_t; opaque: c_pvoid): c_int; cdecl;

procedure rd_kafka_conf_set_open_cb(conf: Prd_kafka_conf_t; open_cb: rd_kafka_conf_set_open_cb_fn); cdecl; external LIB_NAME;
// #endif
{$ENDIF}

(**
 * @brief Sets the application's opaque pointer that will be passed to callbacks
 *)
procedure rd_kafka_conf_set_opaque(conf: Prd_kafka_conf_t; opaque: c_pvoid); cdecl; external LIB_NAME;

(**
 * @brief Retrieves the opaque pointer previously set with rd_kafka_conf_set_opaque()
 *)
function rd_kafka_opaque(const rk: Prd_kafka_t): c_pvoid; cdecl; external LIB_NAME;



(**
 * Sets the default topic configuration to use for automatically
 * subscribed topics (e.g., through pattern-matched topics).
 * The topic config object is not usable after this call.
 *)
procedure rd_kafka_conf_set_default_topic_conf(conf: Prd_kafka_conf_t; tconf: Prd_kafka_topic_conf_t); cdecl; external LIB_NAME;



(**
 * @brief Retrieve configuration value for property \p name.
 *
 * If \p dest is non-NULL the value will be written to \p dest with at
 * most \p dest_size.
 *
 * \p *dest_size is updated to the full length of the value, thus if
 * \p *dest_size initially is smaller than the full length the application
 * may reallocate \p dest to fit the returned \p *dest_size and try again.
 *
 * If \p dest is NULL only the full length of the value is returned.
 *
 * Returns \p RD_KAFKA_CONF_OK if the property name matched, else
 * \p RD_KAFKA_CONF_UNKNOWN.
 *)
function rd_kafka_conf_get(const conf: Prd_kafka_conf_t; const name: c_pchar; dest: c_pchar;
  dest_size: c_psize_t): rd_kafka_conf_res_t; cdecl; external LIB_NAME;


(**
 * @brief Retrieve topic configuration value for property \p name.
 *
 * @sa rd_kafka_conf_get()
 *)
function rd_kafka_topic_conf_get (const conf: Prd_kafka_topic_conf_t; const name: c_pchar; dest: c_pchar;
  dest_size: c_psize_t): rd_kafka_conf_res_t; cdecl; external LIB_NAME;


(**
 * @brief Dump the configuration properties and values of \p conf to an array
 *        with \"key\", \"value\" pairs.
 *
 * The number of entries in the array is returned in \p *cntp.
 *
 * The dump must be freed with `rd_kafka_conf_dump_free()`.
 *)
function rd_kafka_conf_dump(conf: Prd_kafka_conf_t; cntp: c_psize_t): {const} c_ppchar; cdecl; external LIB_NAME;


(**
 * @brief Dump the topic configuration properties and values of \p conf
 *        to an array with \"key\", \"value\" pairs.
 *
 * The number of entries in the array is returned in \p *cntp.
 *
 * The dump must be freed with `rd_kafka_conf_dump_free()`.
 *)
function rd_kafka_topic_conf_dump(conf: Prd_kafka_topic_conf_t; cntp: c_psize_t): {const} c_ppchar; cdecl; external LIB_NAME;

(**
 * @brief Frees a configuration dump returned from `rd_kafka_conf_dump()` or
 *        `rd_kafka_topic_conf_dump().
 *)
procedure rd_kafka_conf_dump_free(const arr: c_ppchar; cnt: c_size_t); cdecl; external LIB_NAME;

(**
 * @brief Prints a table to \p fp of all supported configuration properties,
 *        their default values as well as a description.
 *)
{$IF defined(KAFKA_DELPHI_USE_PATCHED_LIBRDKAFKA)}
procedure rd_kafka_conf_properties_show2(const filename, mode: c_pchar); cdecl; external LIB_NAME;
{$ELSE}
procedure rd_kafka_conf_properties_show(fp: c_pfile); cdecl; external LIB_NAME;
{$ENDIF}

(**@}*)


(**
 * @name Topic configuration
 * @{
 *
 * @brief Topic configuration property interface
 *
 *)


(**
 * @brief Create topic configuration object
 *
 * @sa Same semantics as for rd_kafka_conf_new().
 *)
function rd_kafka_topic_conf_new: Prd_kafka_topic_conf_t; cdecl; external LIB_NAME;


(**
 * @brief Creates a copy/duplicate of topic configuration object \p conf.
 *)
function rd_kafka_topic_conf_dup(const conf: Prd_kafka_topic_conf_t): Prd_kafka_topic_conf_t; cdecl; external LIB_NAME;


(**
 * @brief Destroys a topic conf object.
 *)
procedure rd_kafka_topic_conf_destroy(topic_conf: Prd_kafka_topic_conf_t); cdecl; external LIB_NAME;


(**
 * @brief Sets a single rd_kafka_topic_conf_t value by property name.
 *
 * \p topic_conf should have been previously set up
 * with `rd_kafka_topic_conf_new()`.
 *
 * @returns rd_kafka_conf_res_t to indicate success or failure.
 *)
function rd_kafka_topic_conf_set(conf: Prd_kafka_topic_conf_t; const name, value: c_pchar; errstr: c_pchar;
  errstr_size: c_size_t): rd_kafka_conf_res_t; cdecl; external LIB_NAME;

(**
 * @brief Sets the application's opaque pointer that will be passed to all topic
 * callbacks as the \c rkt_opaque argument.
 *)
procedure rd_kafka_topic_conf_set_opaque(conf: Prd_kafka_topic_conf_t; opaque: c_pvoid); cdecl; external LIB_NAME;


(**
 * @brief \b Producer: Set partitioner callback in provided topic conf object.
 *
 * The partitioner may be called in any thread at any time,
 * it may be called multiple times for the same message/key.
 *
 * Partitioner function constraints:
 *   - MUST NOT call any rd_kafka_*() functions except:
 *       rd_kafka_topic_partition_available()
 *   - MUST NOT block or execute for prolonged periods of time.
 *   - MUST return a value between 0 and partition_cnt-1, or the
 *     special \c RD_KAFKA_PARTITION_UA value if partitioning
 *     could not be performed.
 *)
type
  rd_kafka_topic_conf_set_partitioner_cb_fn = function(const rkt: Prd_kafka_topic_t; const keydata: c_pvoid; keylen: c_size_t;
                                                partition_cnt: c_int32_t; rkt_opaque, msg_opaque: c_pvoid): c_int32_t; cdecl;

procedure rd_kafka_topic_conf_set_partitioner_cb(topic_conf: Prd_kafka_topic_conf_t;
  partitioner: rd_kafka_topic_conf_set_partitioner_cb_fn); cdecl; external LIB_NAME;

(**
 * @brief Check if partition is available (has a leader broker).
 *
 * @returns 1 if the partition is available, else 0.
 *
 * @warning This function must only be called from inside a partitioner function
 *)
function rd_kafka_topic_partition_available(const rkt: Prd_kafka_topic_t; partition: c_int32_t): c_int; cdecl; external LIB_NAME;


(*******************************************************************
 *                                                                 *
 * Partitioners provided by rdkafka                                *
 *                                                                 *
 *******************************************************************)

(**
 * @brief Random partitioner.
 *
 * Will try not to return unavailable partitions.
 *
 * @returns a random partition between 0 and \p partition_cnt - 1.
 *
 *)
function rd_kafka_msg_partitioner_random(const rkt: Prd_kafka_topic_t; const key: c_pvoid; keylen: c_size_t; partition_cnt: c_int32_t;
  opaque, msg_opaque: c_pvoid): c_int32_t; cdecl; external LIB_NAME;

(**
 * @brief Consistent partitioner.
 *
 * Uses consistent hashing to map identical keys onto identical partitions.
 *
 * @returns a \"random\" partition between 0 and \p partition_cnt - 1 based on
 *          the CRC value of the key
 *)
function rd_kafka_msg_partitioner_consistent(const rkt: Prd_kafka_topic_t; const key: c_pvoid; keylen: c_size_t; partition_cnt: c_int32_t;
  opaque, msg_opaque: c_pvoid): c_int32_t; cdecl; external LIB_NAME;

(**
 * @brief Consistent-Random partitioner.
 *
 * This is the default partitioner.
 * Uses consistent hashing to map identical keys onto identical partitions, and
 * messages without keys will be assigned via the random partitioner.
 *
 * @returns a \"random\" partition between 0 and \p partition_cnt - 1 based on
 *          the CRC value of the key (if provided)
 *)
function rd_kafka_msg_partitioner_consistent_random(const rkt: Prd_kafka_topic_t; const key: c_pvoid; keylen: c_size_t;
  partition_cnt: c_int32_t; opaque, msg_opaque: c_pvoid): c_int32_t; cdecl; external LIB_NAME;


(**@}*)



(**
 * @name Main Kafka and Topic object handles
 * @{
 *
 *
 *)




(**
 * @brief Creates a new Kafka handle and starts its operation according to the
 *        specified \p type (\p RD_KAFKA_CONSUMER or \p RD_KAFKA_PRODUCER).
 *
 * \p conf is an optional struct created with `rd_kafka_conf_new()` that will
 * be used instead of the default configuration.
 * The \p conf object is freed by this function on success and must not be used
 * or destroyed by the application sub-sequently.
 * See `rd_kafka_conf_set()` et.al for more information.
 *
 * \p errstr must be a pointer to memory of at least size \p errstr_size where
 * `rd_kafka_new()` may write a human readable error message in case the
 * creation of a new handle fails. In which case the function returns NULL.
 *
 * @remark \b RD_KAFKA_CONSUMER: When a new \p RD_KAFKA_CONSUMER
 *           rd_kafka_t handle is created it may either operate in the
 *           legacy simple consumer mode using the rd_kafka_consume_start()
 *           interface, or the High-level KafkaConsumer API.
 * @remark An application must only use one of these groups of APIs on a given
 *         rd_kafka_t RD_KAFKA_CONSUMER handle.

 *
 * @returns The Kafka handle on success or NULL on error (see \p errstr)
 *
 * @sa To destroy the Kafka handle, use rd_kafka_destroy().
 *)
function rd_kafka_new(_type: rd_kafka_type_t; conf: Prd_kafka_conf_t; errstr: c_pchar; errstr_size: c_size_t): Prd_kafka_t; cdecl;
  external LIB_NAME;


(**
 * @brief Destroy Kafka handle.
 *
 * @remark This is a blocking operation.
 *)
procedure rd_kafka_destroy(rk: Prd_kafka_t); cdecl; external LIB_NAME;



(**
 * @brief Returns Kafka handle name.
 *)
function rd_kafka_name(const rk: Prd_kafka_t): {const} c_pchar; cdecl; external LIB_NAME;


(**
 * @brief Returns this client's broker-assigned group member id
 *
 * @remark This currently requires the high-level KafkaConsumer
 *
 * @returns An allocated string containing the current broker-assigned group
 *          member id, or NULL if not available.
 *          The application must free the string with \p free() or
 *          rd_kafka_mem_free()
 *)
function rd_kafka_memberid(const rk: Prd_kafka_t): c_pchar; cdecl; external LIB_NAME;


(**
 * @brief Creates a new topic handle for topic named \p topic.
 *
 * \p conf is an optional configuration for the topic created with
 * `rd_kafka_topic_conf_new()` that will be used instead of the default
 * topic configuration.
 * The \p conf object is freed by this function and must not be used or
 * destroyed by the application sub-sequently.
 * See `rd_kafka_topic_conf_set()` et.al for more information.
 *
 * Topic handles are refcounted internally and calling rd_kafka_topic_new()
 * again with the same topic name will return the previous topic handle
 * without updating the original handle's configuration.
 * Applications must eventually call rd_kafka_topic_destroy() for each
 * succesfull call to rd_kafka_topic_new() to clear up resources.
 *
 * @returns the new topic handle or NULL on error (use rd_kafka_errno2err()
 *          to convert system \p errno to an rd_kafka_resp_err_t error code.
 *
 * @sa rd_kafka_topic_destroy()
 *)
function rd_kafka_topic_new(rk: Prd_kafka_t; const topic: c_pchar; conf: Prd_kafka_topic_conf_t): Prd_kafka_topic_t; cdecl;
  external LIB_NAME;



(**
 * @brief Loose application's topic handle refcount as previously created
 *        with `rd_kafka_topic_new()`.
 *
 * @remark Since topic objects are refcounted (both internally and for the app)
 *         the topic object might not actually be destroyed by this call,
 *         but the application must consider the object destroyed.
 *)
procedure rd_kafka_topic_destroy(rkt: Prd_kafka_topic_t); cdecl; external LIB_NAME;


(**
 * @brief Returns the topic name.
 *)
function rd_kafka_topic_name(const rkt: Prd_kafka_topic_t): {const} c_pchar; cdecl; external LIB_NAME;


(**
 * @brief Get the \p rkt_opaque pointer that was set in the topic configuration.
 *)
function rd_kafka_topic_opaque(const rkt: Prd_kafka_topic_t): c_pvoid; cdecl; external LIB_NAME;


(**
 * @brief Unassigned partition.
 *
 * The unassigned partition is used by the producer API for messages
 * that should be partitioned using the configured or default partitioner.
 *)
const
  RD_KAFKA_PARTITION_UA: c_int32_t = -1;


(**
 * @brief Polls the provided kafka handle for events.
 *
 * Events will cause application provided callbacks to be called.
 *
 * The \p timeout_ms argument specifies the maximum amount of time
 * (in milliseconds) that the call will block waiting for events.
 * For non-blocking calls, provide 0 as \p timeout_ms.
 * To wait indefinately for an event, provide -1.
 *
 * @remark  An application should make sure to call poll() at regular
 *          intervals to serve any queued callbacks waiting to be called.
 *
 * Events:
 *   - delivery report callbacks  (if dr_cb/dr_msg_cb is configured) [producer]
 *   - error callbacks (rd_kafka_conf_set_error_cb()) [all]
 *   - stats callbacks (rd_kafka_conf_set_stats_cb()) [all]
 *   - throttle callbacks (rd_kafka_conf_set_throttle_cb()) [all]
 *
 * @returns the number of events served.
 *)
function rd_kafka_poll(rk: Prd_kafka_t; timeout_ms: c_int): c_int; cdecl; external LIB_NAME;


(**
 * @brief Cancels the current callback dispatcher (rd_kafka_poll(),
 *        rd_kafka_consume_callback(), etc).
 *
 * A callback may use this to force an immediate return to the calling
 * code (caller of e.g. rd_kafka_poll()) without processing any further
 * events.
 *
 * @remark This function MUST ONLY be called from within a librdkafka callback.
 *)
procedure rd_kafka_yield(rk: Prd_kafka_t); cdecl; external LIB_NAME;




(**
 * @brief Pause producing or consumption for the provided list of partitions.
 *
 * Success or error is returned per-partition \p err in the \p partitions list.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR
 *)
function rd_kafka_pause_partitions(rk: Prd_kafka_t; partitions: Prd_kafka_topic_partition_list_t): rd_kafka_resp_err_t; cdecl;
  external LIB_NAME;



(**
 * @brief Resume producing consumption for the provided list of partitions.
 *
 * Success or error is returned per-partition \p err in the \p partitions list.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR
 *)
function rd_kafka_resume_partitions(rk: Prd_kafka_t; partitions: Prd_kafka_topic_partition_list_t): rd_kafka_resp_err_t; cdecl;
  external LIB_NAME;




(**
 * @brief Query broker for low (oldest/beginning) and high (newest/end) offsets
 *        for partition.
 *
 * Offsets are returned in \p *low and \p *high respectively.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success or an error code on failure.
 *)
function rd_kafka_query_watermark_offsets(rk: Prd_kafka_t; const topic: c_pchar; partition: c_int32_t; low, high: c_pint64_t;
  timeout_ms: c_int): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**
 * @brief Get last known low (oldest/beginning) and high (newest/end) offsets
 *        for partition.
 *
 * The low offset is updated periodically (if statistics.interval.ms is set)
 * while the high offset is updated on each fetched message set from the broker.
 *
 * If there is no cached offset (either low or high, or both) then
 * RD_KAFKA_OFFSET_INVALID will be returned for the respective offset.
 *
 * Offsets are returned in \p *low and \p *high respectively.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success or an error code on failure.
 *
 * @remark Shall only be used with an active consumer instance.
 *)
function rd_kafka_get_watermark_offsets(rk: Prd_kafka_t; const topic: c_pchar; partition: c_int32_t;
  low, high: c_pint64_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;



(**
 * @brief Look up the offsets for the given partitions by timestamp.
 *
 * The returned offset for each partition is the earliest offset whose
 * timestamp is greater than or equal to the given timestamp in the
 * corresponding partition.
 *
 * The timestamps to query are represented as \c offset in \p offsets
 * on input, and \c offset will contain the offset on output.
 *
 * The function will block for at most \p timeout_ms milliseconds.
 *
 * @remark Duplicate Topic+Partitions are not supported.
 * @remark Per-partition errors may be returned in \c rd_kafka_topic_partition_t.err
 *
 * @returns an error code for general errors, else RD_KAFKA_RESP_ERR_NO_ERROR
 *          in which case per-partition errors might be set.
 *)
function rd_kafka_offsets_for_times(rk: Prd_kafka_t; offsets: Prd_kafka_topic_partition_list_t;
  timeout_ms: c_int): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**
 * @brief Free pointer returned by librdkafka
 *
 * This is typically an abstraction for the free(3) call and makes sure
 * the application can use the same memory allocator as librdkafka for
 * freeing pointers returned by librdkafka.
 *
 * In standard setups it is usually not necessary to use this interface
 * rather than the free(3) functione.
 *
 * @remark rd_kafka_mem_free() must only be used for pointers returned by APIs
 *         that explicitly mention using this function for freeing.
 *)
procedure rd_kafka_mem_free(rk: Prd_kafka_t; ptr: c_pvoid); cdecl; external LIB_NAME;


(**@}*)





(**
 * @name Queue API
 * @{
 *
 * Message queues allows the application to re-route consumed messages
 * from multiple topic+partitions into one single queue point.
 * This queue point containing messages from a number of topic+partitions
 * may then be served by a single rd_kafka_consume*_queue() call,
 * rather than one call per topic+partition combination.
 *)


(**
 * @brief Create a new message queue.
 *
 * See rd_kafka_consume_start_queue(), rd_kafka_consume_queue(), et.al.
 *)
function rd_kafka_queue_new(rk: Prd_kafka_t): Prd_kafka_queue_t; cdecl; external LIB_NAME;

(**
 * Destroy a queue, purging all of its enqueued messages.
 *)
procedure rd_kafka_queue_destroy(rkqu: Prd_kafka_queue_t); cdecl; external LIB_NAME;


(**
 * @returns a reference to the main librdkafka event queue.
 * This is the queue served by rd_kafka_poll().
 *
 * Use rd_kafka_queue_destroy() to loose the reference.
 *)
function rd_kafka_queue_get_main(rk: Prd_kafka_t): Prd_kafka_queue_t; cdecl; external LIB_NAME;


(**
 * @returns a reference to the librdkafka consumer queue.
 * This is the queue served by rd_kafka_consumer_poll().
 *
 * Use rd_kafka_queue_destroy() to loose the reference.
 *
 * @remark rd_kafka_queue_destroy() MUST be called on this queue
 *         prior to calling rd_kafka_consumer_close().
 *)
function rd_kafka_queue_get_consumer(rk: Prd_kafka_t): Prd_kafka_queue_t; cdecl; external LIB_NAME;

(**
 * @returns a reference to the partition's queue, or NULL if
 *          partition is invalid.
 *
 * Use rd_kafka_queue_destroy() to loose the reference.
 *
 * @remark rd_kafka_queue_destroy() MUST be called on this queue
 *
 * @remark This function only works on consumers.
 *)
function rd_kafka_queue_get_partition(rk: Prd_kafka_t; const topic: c_pchar; partition: c_int32_t): Prd_kafka_queue_t; cdecl;
  external LIB_NAME;

(**
 * @brief Forward/re-route queue \p src to \p dst.
 * If \p dst is \c NULL the forwarding is removed.
 *
 * The internal refcounts for both queues are increased.
 *
 * @remark Regardless of whether \p dst is NULL or not, after calling this
 *         function, \p src will not forward it's fetch queue to the consumer
 *         queue.
 *)
procedure rd_kafka_queue_forward(src, dst: Prd_kafka_queue_t); cdecl; external LIB_NAME;

(**
 * @brief Forward librdkafka logs (and debug) to the specified queue
 *        for serving with one of the ..poll() calls.
 *
 *        This allows an application to serve log callbacks (\c log_cb)
 *        in its thread of choice.
 *
 * @param rkqu Queue to forward logs to. If the value is NULL the logs
 *        are forwarded to the main queue.
 *
 * @remark The configuration property \c log.queue MUST also be set to true.
 *
 * @remark librdkafka maintains its own reference to the provided queue.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success or an error code on error.
 *)
function rd_kafka_set_log_queue(rk: Prd_kafka_t; rkqu: Prd_kafka_queue_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**
 * @returns the current number of elements in queue.
 *)
function rd_kafka_queue_length(rkqu: Prd_kafka_queue_t): c_size_t; cdecl; external LIB_NAME;


(**
 * @brief Enable IO event triggering for queue.
 *
 * To ease integration with IO based polling loops this API
 * allows an application to create a separate file-descriptor
 * that librdkafka will write \p payload (of size \p size) to
 * whenever a new element is enqueued on a previously empty queue.
 *
 * To remove event triggering call with \p fd = -1.
 *
 * librdkafka will maintain a copy of the \p payload.
 *
 * @remark When using forwarded queues the IO event must only be enabled
 *         on the final forwarded-to (destination) queue.
 *)
procedure rd_kafka_queue_io_event_enable(rkqu: Prd_kafka_queue_t; fd: c_int; const payload: c_pvoid; size: c_size_t); cdecl;
  external LIB_NAME;

(**@}*)

(**
 *
 * @name Simple Consumer API (legacy)
 * @{
 *
 *)


const
  RD_KAFKA_OFFSET_BEGINNING: c_int32_t = -2;    (**< Start consuming from beginning of
                                                 *   kafka partition queue: oldest msg *)
  RD_KAFKA_OFFSET_END: c_int32_t = -1;          (**< Start consuming from end of kafka
                                                 *   partition queue: next msg *)
  RD_KAFKA_OFFSET_STORED: c_int32_t = -1000;    (**< Start consuming from offset retrieved
                                                 *   from offset store *)
  RD_KAFKA_OFFSET_INVALID: c_int32_t = -1001;   (**< Invalid offset *)


(** @cond NO_DOC *)
  RD_KAFKA_OFFSET_TAIL_BASE: c_int32_t = -2000; (* internal: do not use *)
(** @endcond *)

(**
 * @brief Start consuming \p CNT messages from topic's current end offset.
 *
 * That is, if current end offset is 12345 and \p CNT is 200, it will start
 * consuming from offset \c 12345-200 = \c 12145. *)
function RD_KAFKA_OFFSET_TAIL(CNT: c_int32_t): c_int32_t; inline;

(**
 * @brief Start consuming messages for topic \p rkt and \p partition
 * at offset \p offset which may either be an absolute \c (0..N)
 * or one of the logical offsets:
 *  - RD_KAFKA_OFFSET_BEGINNING
 *  - RD_KAFKA_OFFSET_END
 *  - RD_KAFKA_OFFSET_STORED
 *  - RD_KAFKA_OFFSET_TAIL
 *
 * rdkafka will attempt to keep \c queued.min.messages (config property)
 * messages in the local queue by repeatedly fetching batches of messages
 * from the broker until the threshold is reached.
 *
 * The application shall use one of the `rd_kafka_consume*()` functions
 * to consume messages from the local queue, each kafka message being
 * represented as a `rd_kafka_message_t *` object.
 *
 * `rd_kafka_consume_start()` must not be called multiple times for the same
 * topic and partition without stopping consumption first with
 * `rd_kafka_consume_stop()`.
 *
 * @returns 0 on success or -1 on error in which case errno is set accordingly:
 *  - EBUSY    - Conflicts with an existing or previous subscription
 *               (RD_KAFKA_RESP_ERR__CONFLICT)
 *  - EINVAL   - Invalid offset, or incomplete configuration (lacking group.id)
 *               (RD_KAFKA_RESP_ERR__INVALID_ARG)
 *  - ESRCH    - requested \p partition is invalid.
 *               (RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
 *  - ENOENT   - topic is unknown in the Kafka cluster.
 *               (RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
 *
 * Use `rd_kafka_errno2err()` to convert sytem \c errno to `rd_kafka_resp_err_t`
 *)
function rd_kafka_consume_start(rkt: Prd_kafka_topic_t; partition: c_int32_t; offset: c_int64_t): c_int; cdecl; external LIB_NAME;

(**
 * @brief Same as rd_kafka_consume_start() but re-routes incoming messages to
 * the provided queue \p rkqu (which must have been previously allocated
 * with `rd_kafka_queue_new()`.
 *
 * The application must use one of the `rd_kafka_consume_*_queue()` functions
 * to receive fetched messages.
 *
 * `rd_kafka_consume_start_queue()` must not be called multiple times for the
 * same topic and partition without stopping consumption first with
 * `rd_kafka_consume_stop()`.
 * `rd_kafka_consume_start()` and `rd_kafka_consume_start_queue()` must not
 * be combined for the same topic and partition.
 *)
function rd_kafka_consume_start_queue(rkt: Prd_kafka_topic_t; partition: c_int32_t; offset: c_int64_t;
  rkqu: Prd_kafka_queue_t): c_int; cdecl; external LIB_NAME;

(**
 * @brief Stop consuming messages for topic \p rkt and \p partition, purging
 * all messages currently in the local queue.
 *
 * NOTE: To enforce synchronisation this call will block until the internal
 *       fetcher has terminated and offsets are committed to configured
 *       storage method.
 *
 * The application needs to be stop all consumers before calling
 * `rd_kafka_destroy()` on the main object handle.
 *
 * @returns 0 on success or -1 on error (see `errno`).
 *)
function rd_kafka_consume_stop(rkt: Prd_kafka_topic_t; partition: c_int32_t): c_int; cdecl; external LIB_NAME;



(**
 * @brief Seek consumer for topic+partition to \p offset which is either an
 *        absolute or logical offset.
 *
 * If \p timeout_ms is not 0 the call will wait this long for the
 * seek to be performed. If the timeout is reached the internal state
 * will be unknown and this function returns `RD_KAFKA_RESP_ERR__TIMED_OUT`.
 * If \p timeout_ms is 0 it will initiate the seek but return
 * immediately without any error reporting (e.g., async).
 *
 * This call triggers a fetch queue barrier flush.
 *
 * @returns `RD_KAFKA_RESP_ERR__NO_ERROR` on success else an error code.
 *)
function rd_kafka_seek(rkt: Prd_kafka_topic_t; partition: c_int32_t; offset: c_int64_t; timeout_ms: c_int): rd_kafka_resp_err_t; cdecl;
  external LIB_NAME;


(**
 * @brief Consume a single message from topic \p rkt and \p partition
 *
 * \p timeout_ms is maximum amount of time to wait for a message to be received.
 * Consumer must have been previously started with `rd_kafka_consume_start()`.
 *
 * Returns a message object on success or \c NULL on error.
 * The message object must be destroyed with `rd_kafka_message_destroy()`
 * when the application is done with it.
 *
 * Errors (when returning NULL):
 *  - ETIMEDOUT - \p timeout_ms was reached with no new messages fetched.
 *  - ENOENT    - \p rkt + \p partition is unknown.
 *                 (no prior `rd_kafka_consume_start()` call)
 *
 * NOTE: The returned message's \c ..->err must be checked for errors.
 * NOTE: \c ..->err \c == \c RD_KAFKA_RESP_ERR__PARTITION_EOF signals that the
 *       end of the partition has been reached, which should typically not be
 *       considered an error. The application should handle this case
 *       (e.g., ignore).
 *)
function rd_kafka_consume(rkt: Prd_kafka_topic_t; partition: c_int32_t; timeout_ms: c_int): Prd_kafka_message_t; cdecl; external LIB_NAME;



(**
 * @brief Consume up to \p rkmessages_size from topic \p rkt and \p partition
 *        putting a pointer to each message in the application provided
 *        array \p rkmessages (of size \p rkmessages_size entries).
 *
 * `rd_kafka_consume_batch()` provides higher throughput performance
 * than `rd_kafka_consume()`.
 *
 * \p timeout_ms is the maximum amount of time to wait for all of
 * \p rkmessages_size messages to be put into \p rkmessages.
 * If no messages were available within the timeout period this function
 * returns 0 and \p rkmessages remains untouched.
 * This differs somewhat from `rd_kafka_consume()`.
 *
 * The message objects must be destroyed with `rd_kafka_message_destroy()`
 * when the application is done with it.
 *
 * @returns the number of rkmessages added in \p rkmessages,
 * or -1 on error (same error codes as for `rd_kafka_consume()`.
 *
 * @sa rd_kafka_consume()
 *)
function rd_kafka_consume_batch(rkt: Prd_kafka_topic_t; partition: c_int32_t; timeout_ms: c_int; rkmessages: PPrd_kafka_message_t;
  rkmessages_size: c_size_t): c_ssize_t ; cdecl; external LIB_NAME;



(**
 * @brief Consumes messages from topic \p rkt and \p partition, calling
 * the provided callback for each consumed messsage.
 *
 * `rd_kafka_consume_callback()` provides higher throughput performance
 * than both `rd_kafka_consume()` and `rd_kafka_consume_batch()`.
 *
 * \p timeout_ms is the maximum amount of time to wait for one or more messages
 * to arrive.
 *
 * The provided \p consume_cb function is called for each message,
 * the application \b MUST \b NOT call `rd_kafka_message_destroy()` on the
 * provided \p rkmessage.
 *
 * The \p opaque argument is passed to the 'consume_cb' as \p opaque.
 *
 * @returns the number of messages processed or -1 on error.
 *
 * @sa rd_kafka_consume()
 *)
type
  rd_kafka_consume_callback_fn = procedure(rkmessage: Prd_kafka_message_t; opaque: c_pvoid); cdecl;

function rd_kafka_consume_callback(rkt: Prd_kafka_topic_t; partition: c_int32_t; timeout_ms: c_int;
  consume_cb: rd_kafka_consume_callback_fn; opaque: c_pvoid): c_int; cdecl; external LIB_NAME;


(**
 * @name Simple Consumer API (legacy): Queue consumers
 * @{
 *
 * The following `..._queue()` functions are analogue to the functions above
 * but reads messages from the provided queue \p rkqu instead.
 * \p rkqu must have been previously created with `rd_kafka_queue_new()`
 * and the topic consumer must have been started with
 * `rd_kafka_consume_start_queue()` utilising the the same queue.
 *)

(**
 * @brief Consume from queue
 *
 * @sa rd_kafka_consume()
 *)
function rd_kafka_consume_queue(rkqu: Prd_kafka_queue_t; timeout_ms: c_int): Prd_kafka_message_t; cdecl; external LIB_NAME;

(**
 * @brief Consume batch of messages from queue
 *
 * @sa rd_kafka_consume_batch()
 *)
function rd_kafka_consume_batch_queue(rkqu: Prd_kafka_queue_t; timeout_ms: c_int; rkmessages: PPrd_kafka_message_t;
  rkmessages_size: c_size_t): c_ssize_t; cdecl; external LIB_NAME;

(**
 * @brief Consume multiple messages from queue with callback
 *
 * @sa rd_kafka_consume_callback()
 *)
type
  rd_kafka_consume_callback_queue_fn = procedure(rkmessage: Prd_kafka_message_t; opaque: c_pvoid); cdecl;

function rd_kafka_consume_callback_queue(rkqu: Prd_kafka_queue_t; timeout_ms: c_int; consume_cb: rd_kafka_consume_callback_queue_fn;
  opaque: c_pvoid): c_int; cdecl; external LIB_NAME;


(**@}*)




(**
 * @name Simple Consumer API (legacy): Topic+partition offset store.
 * @{
 *
 * If \c auto.commit.enable is true the offset is stored automatically prior to
 * returning of the message(s) in each of the rd_kafka_consume*() functions
 * above.
 *)


(**
 * @brief Store offset \p offset for topic \p rkt partition \p partition.
 *
 * The offset will be committed (written) to the offset store according
 * to \c `auto.commit.interval.ms`.
 *
 * @remark \c `auto.commit.enable` must be set to "false" when using this API.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success or an error code on error.
 *)
function rd_kafka_offset_store(rkt: Prd_kafka_topic_t; partition: c_int32_t; offset: c_int64_t): rd_kafka_resp_err_t; cdecl;
  external LIB_NAME;
(**@}*)




(**
 * @name KafkaConsumer (C)
 * @{
 * @brief High-level KafkaConsumer C API
 *
 *
 *
 *)

(**
 * @brief Subscribe to topic set using balanced consumer groups.
 *
 * Wildcard (regex) topics are supported by the librdkafka assignor:
 * any topic name in the \p topics list that is prefixed with \c \"^\" will
 * be regex-matched to the full list of topics in the cluster and matching
 * topics will be added to the subscription list.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success or
 *          RD_KAFKA_RESP_ERR__INVALID_ARG if list is empty, contains invalid
 *          topics or regexes.
 *)
function rd_kafka_subscribe(rk: Prd_kafka_t; const topics: Prd_kafka_topic_partition_list_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**
 * @brief Unsubscribe from the current subscription set.
 *)
function rd_kafka_unsubscribe(rk: Prd_kafka_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**
 * @brief Returns the current topic subscription
 *
 * @returns An error code on failure, otherwise \p topic is updated
 *          to point to a newly allocated topic list (possibly empty).
 *
 * @remark The application is responsible for calling
 *         rd_kafka_topic_partition_list_destroy on the returned list.
 *)
function rd_kafka_subscription(rk: Prd_kafka_t; topics: PPrd_kafka_topic_partition_list_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;



(**
 * @brief Poll the consumer for messages or events.
 *
 * Will block for at most \p timeout_ms milliseconds.
 *
 * @remark  An application should make sure to call consumer_poll() at regular
 *          intervals, even if no messages are expected, to serve any
 *          queued callbacks waiting to be called. This is especially
 *          important when a rebalance_cb has been registered as it needs
 *          to be called and handled properly to synchronize internal
 *          consumer state.
 *
 * @returns A message object which is a proper message if \p ->err is
 *          RD_KAFKA_RESP_ERR_NO_ERROR, or an event or error for any other
 *          value.
 *
 * @sa rd_kafka_message_t
 *)
function rd_kafka_consumer_poll(rk: Prd_kafka_t; timeout_ms: c_int): Prd_kafka_message_t; cdecl; external LIB_NAME;

(**
 * @brief Close down the KafkaConsumer.
 *
 * @remark This call will block until the consumer has revoked its assignment,
 *         calling the \c rebalance_cb if it is configured, committed offsets
 *         to broker, and left the consumer group.
 *         The maximum blocking time is roughly limited to session.timeout.ms.
 *
 * @returns An error code indicating if the consumer close was succesful
 *          or not.
 *
 * @remark The application still needs to call rd_kafka_destroy() after
 *         this call finishes to clean up the underlying handle resources.
 *
 *)
function rd_kafka_consumer_close(rk: Prd_kafka_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;



(**
 * @brief Atomic assignment of partitions to consume.
 *
 * The new \p partitions will replace the existing assignment.
 *
 * When used from a rebalance callback the application shall pass the
 * partition list passed to the callback (or a copy of it) (even if the list
 * is empty) rather than NULL to maintain internal join state.

 * A zero-length \p partitions will treat the partitions as a valid,
 * albeit empty, assignment, and maintain internal state, while a \c NULL
 * value for \p partitions will reset and clear the internal state.
 *)
function rd_kafka_assign(rk: Prd_kafka_t; const partitions: Prd_kafka_topic_partition_list_t): rd_kafka_resp_err_t; cdecl;
  external LIB_NAME;

(**
 * @brief Returns the current partition assignment
 *
 * @returns An error code on failure, otherwise \p partitions is updated
 *          to point to a newly allocated partition list (possibly empty).
 *
 * @remark The application is responsible for calling
 *         rd_kafka_topic_partition_list_destroy on the returned list.
 *)
function rd_kafka_assignment(rk: Prd_kafka_t; partitions: PPrd_kafka_topic_partition_list_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;




(**
 * @brief Commit offsets on broker for the provided list of partitions.
 *
 * \p offsets should contain \c topic, \c partition, \c offset and possibly
 * \c metadata.
 * If \p offsets is NULL the current partition assignment will be used instead.
 *
 * If \p async is false this operation will block until the broker offset commit
 * is done, returning the resulting success or error code.
 *
 * If a rd_kafka_conf_set_offset_commit_cb() offset commit callback has been
 * configured the callback will be enqueued for a future call to
 * rd_kafka_poll(), rd_kafka_consumer_poll() or similar.
 *)
function rd_kafka_commit(rk: Prd_kafka_t; const offsets: Prd_kafka_topic_partition_list_t; async: c_int): rd_kafka_resp_err_t; cdecl;
  external LIB_NAME;


(**
 * @brief Commit message's offset on broker for the message's partition.
 *
 * @sa rd_kafka_commit
 *)
function rd_kafka_commit_message(rk: Prd_kafka_t; const rkmessage: Prd_kafka_message_t; async: c_int): rd_kafka_resp_err_t; cdecl;
  external LIB_NAME;


(**
 * @brief Commit offsets on broker for the provided list of partitions.
 *
 * See rd_kafka_commit for \p offsets semantics.
 *
 * The result of the offset commit will be posted on the provided \p rkqu queue.
 *
 * If the application uses one of the poll APIs (rd_kafka_poll(),
 * rd_kafka_consumer_poll(), rd_kafka_queue_poll(), ..) to serve the queue
 * the \p cb callback is required. \p opaque is passed to the callback.
 *
 * If using the event API the callback is ignored and the offset commit result
 * will be returned as an RD_KAFKA_EVENT_COMMIT event. The \p opaque
 * value will be available with rd_kafka_event_opaque()
 *
 * If \p rkqu is NULL a temporary queue will be created and the callback will
 * be served by this call.
 *
 * @sa rd_kafka_commit()
 * @sa rd_kafka_conf_set_offset_commit_cb()
 *)
type
  rd_kafka_commit_queue_fn = procedure(rk: Prd_kafka_t; err: rd_kafka_resp_err_t; offsets: Prd_kafka_topic_partition_list_t;
                               opaque: c_pvoid); cdecl;

function rd_kafka_commit_queue(rk: Prd_kafka_t; const offsets: Prd_kafka_topic_partition_list_t; rkqu: Prd_kafka_queue_t;
  cb: rd_kafka_commit_queue_fn; opaque: c_pvoid): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**
 * @brief Retrieve committed offsets for topics+partitions.
 *
 * The \p offset field of each requested partition will either be set to
 * stored offset or to RD_KAFKA_OFFSET_INVALID in case there was no stored
 * offset for that partition.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success in which case the
 *          \p offset or \p err field of each \p partitions' element is filled
 *          in with the stored offset, or a partition specific error.
 *          Else returns an error code.
 *)
function rd_kafka_committed(rk: Prd_kafka_t; partitions: Prd_kafka_topic_partition_list_t; timeout_ms: c_int): rd_kafka_resp_err_t; cdecl;
  external LIB_NAME;



(**
 * @brief Retrieve current positions (offsets) for topics+partitions.
 *
 * The \p offset field of each requested partition will be set to the offset
 * of the last consumed message + 1, or RD_KAFKA_OFFSET_INVALID in case there was
 * no previous message.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success in which case the
 *          \p offset or \p err field of each \p partitions' element is filled
 *          in with the stored offset, or a partition specific error.
 *          Else returns an error code.
 *)
function rd_kafka_position(rk: Prd_kafka_t; partitions: Prd_kafka_topic_partition_list_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**@}*)



(**
 * @name Producer API
 * @{
 *
 *
 *)


(**
 * @brief Producer message flags
 *)
const
  RD_KAFKA_MSG_F_FREE: c_int32_t = $1;  (**< Delegate freeing of payload to rdkafka. *)
  RD_KAFKA_MSG_F_COPY: c_int32_t = $2;  (**< rdkafka will make a copy of the payload. *)
  RD_KAFKA_MSG_F_BLOCK: c_int32_t = $4; (**< Block produce*() on message queue full.
                                         *   WARNING: If a delivery report callback
                                         *            is used the application MUST
                                         *            call rd_kafka_poll() (or equiv.)
                                         *            to make sure delivered messages
                                         *            are drained from the internal
                                         *            delivery report queue.
                                         *            Failure to do so will result
                                         *            in indefinately blocking on
                                         *            the produce() call when the
                                         *            message queue is full.
                                         *)



(**
 * @brief Produce and send a single message to broker.
 *
 * \p rkt is the target topic which must have been previously created with
 * `rd_kafka_topic_new()`.
 *
 * `rd_kafka_produce()` is an asynch non-blocking API.
 *
 * \p partition is the target partition, either:
 *   - RD_KAFKA_PARTITION_UA (unassigned) for
 *     automatic partitioning using the topic's partitioner function, or
 *   - a fixed partition (0..N)
 *
 * \p msgflags is zero or more of the following flags OR:ed together:
 *    RD_KAFKA_MSG_F_BLOCK - block \p produce*() call if
 *                           \p queue.buffering.max.messages or
 *                           \p queue.buffering.max.kbytes are exceeded.
 *                           Messages are considered in-queue from the point they
 *                           are accepted by produce() until their corresponding
 *                           delivery report callback/event returns.
 *                           It is thus a requirement to call
 *                           rd_kafka_poll() (or equiv.) from a separate
 *                           thread when F_BLOCK is used.
 *                           See WARNING on \c RD_KAFKA_MSG_F_BLOCK above.
 *
 *    RD_KAFKA_MSG_F_FREE - rdkafka will free(3) \p payload when it is done
 *                          with it.
 *    RD_KAFKA_MSG_F_COPY - the \p payload data will be copied and the
 *                          \p payload pointer will not be used by rdkafka
 *                          after the call returns.
 *
 *    .._F_FREE and .._F_COPY are mutually exclusive.
 *
 *    If the function returns -1 and RD_KAFKA_MSG_F_FREE was specified, then
 *    the memory associated with the payload is still the caller's
 *    responsibility.
 *
 * \p payload is the message payload of size \p len bytes.
 *
 * \p key is an optional message key of size \p keylen bytes, if non-NULL it
 * will be passed to the topic partitioner as well as be sent with the
 * message to the broker and passed on to the consumer.
 *
 * \p msg_opaque is an optional application-provided per-message opaque
 * pointer that will provided in the delivery report callback (`dr_cb`) for
 * referencing this message.
 *
 * Returns 0 on success or -1 on error in which case errno is set accordingly:
 *  - ENOBUFS  - maximum number of outstanding messages has been reached:
 *               "queue.buffering.max.messages"
 *               (RD_KAFKA_RESP_ERR__QUEUE_FULL)
 *  - EMSGSIZE - message is larger than configured max size:
 *               "messages.max.bytes".
 *               (RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE)
 *  - ESRCH    - requested \p partition is unknown in the Kafka cluster.
 *               (RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
 *  - ENOENT   - topic is unknown in the Kafka cluster.
 *               (RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
 *
 * @sa Use rd_kafka_errno2err() to convert `errno` to rdkafka error code.
 *)
function rd_kafka_produce(rkt: Prd_kafka_topic_t; partition: c_int32_t; msgflags: c_int; payload: c_pvoid; len: c_size_t;
  const key: c_pvoid; keylen: c_size_t; msg_opaque: c_pvoid): c_int; cdecl; external LIB_NAME;


(**
 * @brief Produce and send a single message to broker.
 *
 * The message is defined by a va-arg list using \c rd_kafka_vtype_t
 * tag tuples which must be terminated with a single \c RD_KAFKA_V_END.
 *
 * @returns \c RD_KAFKA_RESP_ERR_NO_ERROR on success, else an error code.
 *
 * @sa rd_kafka_produce, RD_KAFKA_V_END
 *)
function rd_kafka_producev(rk: Prd_kafka_t): rd_kafka_resp_err_t; cdecl; varargs; external LIB_NAME;


(**
 * @brief Produce multiple messages.
 *
 * If partition is RD_KAFKA_PARTITION_UA the configured partitioner will
 * be run for each message (slower), otherwise the messages will be enqueued
 * to the specified partition directly (faster).
 *
 * The messages are provided in the array \p rkmessages of count \p message_cnt
 * elements.
 * The \p partition and \p msgflags are used for all provided messages.
 *
 * Honoured \p rkmessages[] fields are:
 *  - payload,len    Message payload and length
 *  - key,key_len    Optional message key
 *  - _private       Message opaque pointer (msg_opaque)
 *  - err            Will be set according to success or failure.
 *                   Application only needs to check for errors if
 *                   return value != \p message_cnt.
 *
 * @returns the number of messages succesfully enqueued for producing.
 *)
function rd_kafka_produce_batch(rkt: Prd_kafka_topic_t; partition: c_int32_t; msgflags: c_int; rkmessages: Prd_kafka_message_t;
  message_cnt: c_int): c_int; cdecl; external LIB_NAME;




(**
 * @brief Wait until all outstanding produce requests, et.al, are completed.
 *        This should typically be done prior to destroying a producer instance
 *        to make sure all queued and in-flight produce requests are completed
 *        before terminating.
 *
 * @remark This function will call rd_kafka_poll() and thus trigger callbacks.
 *
 * @returns RD_KAFKA_RESP_ERR__TIMED_OUT if \p timeout_ms was reached before all
 *          outstanding requests were completed, else RD_KAFKA_RESP_ERR_NO_ERROR
 *)
function rd_kafka_flush(rk: Prd_kafka_t; timeout_ms: c_int): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**@}*)


(**
* @name Metadata API
* @{
*
*
*)


(**
 * @brief Broker information
 *)
type
  // C tag: rd_kafka_metadata_broker
  rd_kafka_metadata_broker_t = record
    id: c_int32_t; (**< Broker Id *)
    host: c_pchar; (**< Broker hostname *)
    port: c_int;   (**< Broker listening port *)
  end;
  Prd_kafka_metadata_broker_t = ^rd_kafka_metadata_broker_t;

(**
 * @brief Partition information
 *)
  // C tag: rd_kafka_metadata_partition
  rd_kafka_metadata_partition_t = record
    id: c_int32_t;            (**< Partition Id *)
    err: rd_kafka_resp_err_t; (**< Partition error reported by broker *)
    leader: c_int32_t;        (**< Leader broker *)
    replica_cnt: c_int;       (**< Number of brokers in \p replicas *)
    replicas: c_pint32_t;     (**< Replica brokers *)
    isr_cnt: c_int;           (**< Number of ISR brokers in \p isrs *)
    isrs: c_pint32_t;         (**< In-Sync-Replica brokers *)
  end;
  Prd_kafka_metadata_partition_t = ^rd_kafka_metadata_partition_t;

(**
 * @brief Topic information
 *)
  // C tag: rd_kafka_metadata_topic
  rd_kafka_metadata_topic_t = record
    topic: c_pchar;                             (**< Topic name *)
    partition_cnt: c_int;                       (**< Number of partitions in \p partitions*)
    partitions: Prd_kafka_metadata_partition_t; (**< Partitions *)
    err: rd_kafka_resp_err_t;                   (**< Topic error reported by broker *)
  end;
  Prd_kafka_metadata_topic_t = ^rd_kafka_metadata_topic_t;


(**
 * @brief Metadata container
 *)
  // C tag: rd_kafka_metadata
  rd_kafka_metadata_t = record
    broker_cnt: c_int;                    (**< Number of brokers in \p brokers *)
    brokers: Prd_kafka_metadata_broker_t; (**< Brokers *)

    topic_cnt: c_int;                     (**< Number of topics in \p topics *)
    topics: Prd_kafka_metadata_topic_t;   (**< Topics *)

    orig_broker_id: c_int32_t;            (**< Broker originating this metadata *)
    orig_broker_name: c_pchar;            (**< Name of originating broker *)
  end;
  Prd_kafka_metadata_t = ^rd_kafka_metadata_t;
  PPrd_kafka_metadata_t = ^Prd_kafka_metadata_t;


(**
 * @brief Request Metadata from broker.
 *
 * Parameters:
 *  - \p all_topics  if non-zero: request info about all topics in cluster,
 *                   if zero: only request info about locally known topics.
 *  - \p only_rkt    only request info about this topic
 *  - \p metadatap   pointer to hold metadata result.
 *                   The \p *metadatap pointer must be released
 *                   with rd_kafka_metadata_destroy().
 *  - \p timeout_ms  maximum response time before failing.
 *
 * Returns RD_KAFKA_RESP_ERR_NO_ERROR on success (in which case *metadatap)
 * will be set, else RD_KAFKA_RESP_ERR__TIMED_OUT on timeout or
 * other error code on error.
 *)
function rd_kafka_metadata(rk: Prd_kafka_t; all_topics: c_int; only_rkt: Prd_kafka_topic_t; const metadatap: PPrd_kafka_metadata_t;
  timeout_ms: c_int): rd_kafka_resp_err_t; cdecl; external LIB_NAME;

(**
 * @brief Release metadata memory.
 *)
procedure rd_kafka_metadata_destroy(const metadata: Prd_kafka_metadata_t); cdecl; external LIB_NAME;


(**@}*)



(**
* @name Client group information
* @{
*
*
*)


(**
 * @brief Group member information
 *
 * For more information on \p member_metadata format, see
 * https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-GroupMembershipAPI
 *
 *)
type
  rd_kafka_group_member_info = record
    member_id: c_pchar;            (**< Member id (generated by broker) *)
    client_id: c_pchar;            (**< Client's \p client.id *)
    client_host: c_pchar;          (**< Client's hostname *)
    member_metadata: c_pvoid;      (**< Member metadata (binary),
                                    *   format depends on \p protocol_type. *)
    member_metadata_size: c_int;   (**< Member metadata size in bytes *)
    member_assignment: c_pvoid;    (**< Member assignment (binary),
                                    *    format depends on \p protocol_type. *)
    member_assignment_size: c_int; (**< Member assignment size in bytes *)
  end;
  Prd_kafka_group_member_info = ^rd_kafka_group_member_info;

(**
 * @brief Group information
 *)
  rd_kafka_group_info = record
    broker: rd_kafka_metadata_broker_t;   (**< Originating broker info *)
    group: c_pchar;                       (**< Group name *)
    err: rd_kafka_resp_err_t;             (**< Broker-originated error *)
    state: c_pchar;                       (**< Group state *)
    protocol_type: c_pchar;               (**< Group protocol type *)
    protocol: c_pchar;                    (**< Group protocol *)
    members: Prd_kafka_group_member_info; (**< Group members *)
    member_cnt: c_int;                    (**< Group member count *)
  end;
  Prd_kafka_group_info = ^rd_kafka_group_info;

(**
 * @brief List of groups
 *
 * @sa rd_kafka_group_list_destroy() to release list memory.
 *)
  rd_kafka_group_list = record
    groups: Prd_kafka_group_info; (**< Groups *)
    group_cnt: c_int;             (**< Group count *)
  end;
  Prd_kafka_group_list = ^rd_kafka_group_list;
  PPrd_kafka_group_list = ^Prd_kafka_group_list;


(**
 * @brief List and describe client groups in cluster.
 *
 * \p group is an optional group name to describe, otherwise (\p NULL) all
 * groups are returned.
 *
 * \p timeout_ms is the (approximate) maximum time to wait for response
 * from brokers and must be a positive value.
 *
 * @returns \p RD_KAFKA_RESP_ERR__NO_ERROR on success and \p grplistp is
 *           updated to point to a newly allocated list of groups.
 *           Else returns an error code on failure and \p grplistp remains
 *           untouched.
 *
 * @sa Use rd_kafka_group_list_destroy() to release list memory.
 *)
function rd_kafka_list_groups(rk: Prd_kafka_t; const group: c_pchar; const grplistp: PPrd_kafka_group_list;
  timeout_ms: c_int): rd_kafka_resp_err_t; cdecl; external LIB_NAME;

(**
 * @brief Release list memory
 *)
procedure rd_kafka_group_list_destroy(const grplist: Prd_kafka_group_list); cdecl; external LIB_NAME;


(**@}*)



(**
 * @name Miscellaneous APIs
 * @{
 *
 *)


(**
 * @brief Adds one or more brokers to the kafka handle's list of initial
 *        bootstrap brokers.
 *
 * Additional brokers will be discovered automatically as soon as rdkafka
 * connects to a broker by querying the broker metadata.
 *
 * If a broker name resolves to multiple addresses (and possibly
 * address families) all will be used for connection attempts in
 * round-robin fashion.
 *
 * \p brokerlist is a ,-separated list of brokers in the format:
 *   \c \<broker1\>,\<broker2\>,..
 * Where each broker is in either the host or URL based format:
 *   \c \<host\>[:\<port\>]
 *   \c \<proto\>://\<host\>[:port]
 * \c \<proto\> is either \c PLAINTEXT, \c SSL, \c SASL, \c SASL_PLAINTEXT
 * The two formats can be mixed but ultimately the value of the
 * `security.protocol` config property decides what brokers are allowed.
 *
 * Example:
 *    brokerlist = "broker1:10000,broker2"
 *    brokerlist = "SSL://broker3:9000,ssl://broker2"
 *
 * @returns the number of brokers successfully added.
 *
 * @remark Brokers may also be defined with the \c metadata.broker.list or
 *         \c bootstrap.servers configuration property (preferred method).
 *)
function rd_kafka_brokers_add(rk: Prd_kafka_t; const brokerlist: c_pchar): c_int; cdecl; external LIB_NAME;




(**
 * @brief Set logger function.
 *
 * The default is to print to stderr, but a syslog logger is also available,
 * see rd_kafka_log_(print|syslog) for the builtin alternatives.
 * Alternatively the application may provide its own logger callback.
 * Or pass 'func' as NULL to disable logging.
 *
 * @deprecated Use rd_kafka_conf_set_log_cb()
 *
 * @remark \p rk may be passed as NULL in the callback.
 *)
type
  rd_kafka_set_logger_fn = procedure(const rk: Prd_kafka_t; level: c_int; const fac, buf: c_pchar); cdecl;

procedure rd_kafka_set_logger(rk: Prd_kafka_t; func: rd_kafka_set_logger_fn); cdecl; external LIB_NAME;
  deprecated 'Use rd_kafka_conf_set_log_cb()';


(**
 * @brief Specifies the maximum logging level produced by
 *        internal kafka logging and debugging.
 *
 * If the \p \"debug\" configuration property is set the level is automatically
 * adjusted to \c LOG_DEBUG (7).
 *)
procedure rd_kafka_set_log_level(rk: Prd_kafka_t; level: c_int); cdecl; external LIB_NAME;


(**
 * @brief Builtin (default) log sink: print to stderr
 *)
procedure rd_kafka_log_print(const rk: Prd_kafka_t; level: c_int; const fac, buf: c_pchar); cdecl; external LIB_NAME;


(**
 * @brief Builtin log sink: print to syslog.
 *)
procedure rd_kafka_log_syslog(const rk: Prd_kafka_t; level: c_int; const fac, buf: c_pchar); cdecl; external LIB_NAME;


(**
 * @brief Returns the current out queue length.
 *
 * The out queue contains messages waiting to be sent to, or acknowledged by,
 * the broker.
 *
 * An application should wait for this queue to reach zero before terminating
 * to make sure outstanding requests (such as offset commits) are fully
 * processed.
 *
 * @returns number of messages in the out queue.
 *)
function rd_kafka_outq_len(rk: Prd_kafka_t): c_int; cdecl; external LIB_NAME;



(**
 * @brief Dumps rdkafka's internal state for handle \p rk to stream \p fp
 *
 * This is only useful for debugging rdkafka, showing state and statistics
 * for brokers, topics, partitions, etc.
 *)
{$IF defined(KAFKA_DELPHI_USE_PATCHED_LIBRDKAFKA)}
procedure rd_kafka_dump2(const filename, mode: c_pchar; rk: Prd_kafka_t); cdecl; external LIB_NAME;
{$ELSE}
procedure rd_kafka_dump(fp: c_pfile; rk: Prd_kafka_t); cdecl; external LIB_NAME;
{$ENDIF}



(**
 * @brief Retrieve the current number of threads in use by librdkafka.
 *
 * Used by regression tests.
 *)
function rd_kafka_thread_cnt: c_int; cdecl; external LIB_NAME;


(**
 * @brief Wait for all rd_kafka_t objects to be destroyed.
 *
 * Returns 0 if all kafka objects are now destroyed, or -1 if the
 * timeout was reached.
 * Since `rd_kafka_destroy()` is an asynch operation the
 * `rd_kafka_wait_destroyed()` function can be used for applications where
 * a clean shutdown is required.
 *)
function rd_kafka_wait_destroyed(timeout_ms: c_int): c_int; cdecl; external LIB_NAME;


(**@}*)




(**
 * @name Experimental APIs
 * @{
 *)

(**
 * @brief Redirect the main (rd_kafka_poll()) queue to the KafkaConsumer's
 *        queue (rd_kafka_consumer_poll()).
 *
 * @warning It is not permitted to call rd_kafka_poll() after directing the
 *          main queue with rd_kafka_poll_set_consumer().
 *)
function rd_kafka_poll_set_consumer(rk: Prd_kafka_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**@}*)

(**
 * @name Event interface
 *
 * @brief The event API provides an alternative pollable non-callback interface
 *        to librdkafka's message and event queues.
 *
 * @{
 *)


(**
 * @brief Event types
 *)
type
  rd_kafka_event_type_t = c_int;

const
  RD_KAFKA_EVENT_NONE: c_int32_t = $0;
  RD_KAFKA_EVENT_DR: c_int32_t = $1;             (**< Producer Delivery report batch *)
  RD_KAFKA_EVENT_FETCH: c_int32_t = $2;          (**< Fetched message (consumer) *)
  RD_KAFKA_EVENT_LOG_: c_int32_t = $4;           (**< Log message *)
  RD_KAFKA_EVENT_ERROR_: c_int32_t = $8;         (**< Error *)
  RD_KAFKA_EVENT_REBALANCE: c_int32_t = $10;     (**< Group rebalance (consumer) *)
  RD_KAFKA_EVENT_OFFSET_COMMIT: c_int32_t = $20; (**< Offset commit result *)


type
  // typedef struct rd_kafka_op_s rd_kafka_event_t;
  rd_kafka_op_s = record
  end;
  rd_kafka_event_t = rd_kafka_op_s;
  Prd_kafka_event_t = ^rd_kafka_event_t;


(**
 * @returns the event type for the given event.
 *
 * @remark As a convenience it is okay to pass \p rkev as NULL in which case
 *         RD_KAFKA_EVENT_NONE is returned.
 *)
function rd_kafka_event_type(const rkev: Prd_kafka_event_t): rd_kafka_event_type_t; cdecl; external LIB_NAME;

(**
 * @returns the event type's name for the given event.
 *
 * @remark As a convenience it is okay to pass \p rkev as NULL in which case
 *         the name for RD_KAFKA_EVENT_NONE is returned.
 *)
function rd_kafka_event_name(const rkev: Prd_kafka_event_t): {const} c_pchar; cdecl; external LIB_NAME;


(**
 * @brief Destroy an event.
 *
 * @remark Any references to this event, such as extracted messages,
 *         will not be usable after this call.
 *
 * @remark As a convenience it is okay to pass \p rkev as NULL in which case
 *         no action is performed.
 *)
procedure rd_kafka_event_destroy(rkev: Prd_kafka_event_t); cdecl; external LIB_NAME;


(**
 * @returns the next message from an event.
 *
 * Call repeatedly until it returns NULL.
 *
 * Event types:
 *  - RD_KAFKA_EVENT_FETCH  (1 message)
 *  - RD_KAFKA_EVENT_DR     (>=1 message(s))
 *
 * @remark The returned message(s) MUST NOT be
 *         freed with rd_kafka_message_destroy().
 *)
function rd_kafka_event_message_next(rkev: Prd_kafka_event_t): {const} Prd_kafka_message_t; cdecl; external LIB_NAME;


(**
 * @brief Extacts \p size message(s) from the event into the
 *        pre-allocated array \p rkmessages.
 *
 * Event types:
 *  - RD_KAFKA_EVENT_FETCH  (1 message)
 *  - RD_KAFKA_EVENT_DR     (>=1 message(s))
 *
 * @returns the number of messages extracted.
 *)
function rd_kafka_event_message_array(rkev: Prd_kafka_event_t; const rkmessages: PPrd_kafka_message_t; size: c_size_t): c_size_t; cdecl;
  external LIB_NAME;


(**
 * @returns the number of remaining messages in the event.
 *
 * Event types:
 *  - RD_KAFKA_EVENT_FETCH  (1 message)
 *  - RD_KAFKA_EVENT_DR     (>=1 message(s))
 *)
function rd_kafka_event_message_count(rkev: Prd_kafka_event_t): c_size_t; cdecl; external LIB_NAME;


(**
 * @returns the error code for the event.
 *
 * Event types:
 *  - all
 *)
function rd_kafka_event_error(rkev: Prd_kafka_event_t): rd_kafka_resp_err_t; cdecl; external LIB_NAME;


(**
 * @returns the error string (if any).
 *          An application should check that rd_kafka_event_error() returns
 *          non-zero before calling this function.
 *
 * Event types:
 *  - all
 *)
function rd_kafka_event_error_string(rkev: Prd_kafka_event_t): {const} c_pchar; cdecl; external LIB_NAME;



(**
 * @returns the user opaque (if any)
 *
 * Event types:
 *  - RD_KAFKA_OFFSET_COMMIT
 *)
function rd_kafka_event_opaque(rkev: Prd_kafka_event_t): c_pvoid; cdecl; external LIB_NAME;


(**
 * @brief Extract log message from the event.
 *
 * Event types:
 *  - RD_KAFKA_EVENT_LOG
 *
 * @returns 0 on success or -1 if unsupported event type.
 *)
function rd_kafka_event_log(rkev: Prd_kafka_event_t; const fac, str: c_ppchar; level: c_pint): c_int; cdecl; external LIB_NAME;


(**
 * @returns the topic partition list from the event.
 *
 * @remark The list MUST NOT be freed with rd_kafka_topic_partition_list_destroy()
 *
 * Event types:
 *  - RD_KAFKA_EVENT_REBALANCE
 *  - RD_KAFKA_EVENT_OFFSET_COMMIT
 *)
function rd_kafka_event_topic_partition_list(rkev: Prd_kafka_event_t): Prd_kafka_topic_partition_list_t; cdecl; external LIB_NAME;


(**
 * @returns a newly allocated topic_partition container, if applicable for the event type,
 *          else NULL.
 *
 * @remark The returned pointer MUST be freed with rd_kafka_topic_partition_destroy().
 *
 * Event types:
 *   RD_KAFKA_EVENT_ERROR  (for partition level errors)
 *)
function rd_kafka_event_topic_partition(rkev: Prd_kafka_event_t): Prd_kafka_topic_partition_t; cdecl; external LIB_NAME;


(**
 * @brief Poll a queue for an event for max \p timeout_ms.
 *
 * @returns an event, or NULL.
 *
 * @remark Use rd_kafka_event_destroy() to free the event.
 *)
function rd_kafka_queue_poll(rkqu: Prd_kafka_queue_t; timeout_ms: c_int): Prd_kafka_event_t; cdecl; external LIB_NAME;

(**
* @brief Poll a queue for events served through callbacks for max \p timeout_ms.
*
* @returns the number of events served.
*
* @remark This API must only be used for queues with callbacks registered
*         for all expected event types. E.g., not a message queue.
*)
function rd_kafka_queue_poll_callback(rkqu: Prd_kafka_queue_t; timeout_ms: c_int): c_int; cdecl; external LIB_NAME;


(**@}*)


implementation

function rd_kafka_message_errstr(const rkmessage: Prd_kafka_message_t): {const} c_pchar;
begin
  if Ord(rkmessage.err) = 0 then
    Exit(nil);

  if rkmessage.payload <> nil then
    Exit(rkmessage.payload);

  Result := rd_kafka_err2str(rkmessage.err);
end;

// #define RD_KAFKA_OFFSET_TAIL(CNT)  (RD_KAFKA_OFFSET_TAIL_BASE - (CNT))
function RD_KAFKA_OFFSET_TAIL(CNT: c_int32_t): c_int32_t;
begin
  Result := RD_KAFKA_OFFSET_TAIL_BASE - CNT;
end;

end.
