//
//  Enumerations.swift
//  Franz
//
//  Created by Kellan Cummings on 1/21/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

enum KafkaProtocolError: ErrorType {
    case BadMessageLength
    case MalformedClientId
}


enum AssignmentStrategy: String {
    case Range = "org.apache.kafka.clients.consumer.RangeAssignor"
}

/*
Used to ask for all messages before a certain time (ms).
Pulled in descending order.
*/
enum TimeOffset {
    case Latest  /* the latest offset (i.e. the offset of the next coming message) */
    case Earliest  /* earliest available offset; will always return a single element */
    case Preceding(ms: Int64)
    
    var value: Int64 {
        switch self {
        case .Latest:
            return -1
        case .Earliest:
            return -2
        case Preceding(let ms):
            return ms
        }
    }
}


/*
the minimum number of bytes of messages that must be available to give a response.
*/
enum MinBytes {
    case None
    case One
    case Many(bytes: Int32)
    
    var value: Int32 {
        switch self {
        case .None:
            return 0
        case .One:
            return 1
        case .Many(let bytes):
            return bytes
        }
    }
}


enum ReplicaId {
    case None
    case Node(id: Int32)
    case Debug
    
    var value: Int32 {
        switch self {
        case None:
            return -1
        case .Debug:
            return -2
        case .Node(let id):
            return id
        }
    }
}


enum KafkaErrorCode: Int16 {
    case NoError = 0
    case Unknown = -1
    case OffsetOutOfRange = 1
    case InvalidMessage = 2
    case UnknownTopicOrPartition = 3
    case InvalidMessageSize = 4
    case LeaderNotAvailable = 5
    case NotLeaderForPartition = 6
    case RequestTimedOut = 7
    case BrokerNotAvailable = 8
    case ReplicaNotAvailable = 9
    case MessageSizeTooLarge = 10
    case StaleControllerEpochCode = 11
    case OffsetMetadataTooLargeCode = 12
    case GroupLoadInProgressCode = 14
    case GroupCoordinatorNotAvailableCode = 15
    case NotCoordinatorForGroupCode = 16
    case InvalidTopicCode = 17
    case RecordListTooLargeCode = 18
    case NotEnoughReplicasCode = 19
    case NotEnoughReplicasAfterAppendCode = 20
    case InvalidRequiredAcksCode = 21
    case IllegalGenerationCode = 22
    case InconsistentGroupProtocolCode = 23
    case InvalidGroupIdCode = 24
    case UnknownMemberIdCode = 25
    case InvalidSessionTimeoutCode = 26
    case RebalanceInProgressCode = 27
    case InvalidCommitOffsetSizeCode = 28
    case TopicAuthorizationFailedCode = 29
    case GroupAuthorizationFailedCode = 30
    case ClusterAuthorizationFailedCode = 31
    
    var code: Int {
        return Int(self.rawValue)
    }
    
    var description: String {
        switch self {
        case NoError: return "No error--it worked!"
        case Unknown: return "An unexpected server error"
        case OffsetOutOfRange: return "The requested offset is outside the range of offsets maintained by the server for the given topic/partition."
        case InvalidMessage: return "This indicates that a message contents does not match its CRC"
        case UnknownTopicOrPartition: return "This request is for a topic or partition that does not exist on this broker."
        case InvalidMessageSize: return "The message has a negative size"
        case LeaderNotAvailable: return "This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes."
        case NotLeaderForPartition: return "This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date."
        case RequestTimedOut: return "This error is thrown if the request exceeds the user-specified time limit in the request."
        case BrokerNotAvailable: return "This is not a client facing error and is used mostly by tools when a broker is not alive."
        case ReplicaNotAvailable: return "If replica is expected on a broker, but is not (this can be safely ignored)."
        case MessageSizeTooLarge: return "The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum."
        case StaleControllerEpochCode: return "Internal error code for broker-to-broker communication."
        case OffsetMetadataTooLargeCode: return "If you specify a string larger than configured maximum for offset metadata"
        case GroupLoadInProgressCode: return "The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator."
        case GroupCoordinatorNotAvailableCode: return "The broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created, or if the group coordinator is not active."
        case NotCoordinatorForGroupCode: return "The broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for."
        case InvalidTopicCode: return "For a request which attempts to access an invalid topic (e.g. one which has an illegal name), or if an attempt is made to write to an internal topic (such as the consumer offsets topic)."
        case RecordListTooLargeCode: return "If a message batch in a produce request exceeds the maximum configured segment size."
        case NotEnoughReplicasCode: return "Returned from a produce request when the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1."
        case NotEnoughReplicasAfterAppendCode: return "Returned from a produce request when the message was written to the log, but with fewer in-sync replicas than required."
        case InvalidRequiredAcksCode: return "Returned from a produce request if the requested requiredAcks is invalid (anything other than -1, 1, or 0)."
        case IllegalGenerationCode: return "Returned from group membership requests (such as heartbeats) when the generation id provided in the request is not the current generation."
        case InconsistentGroupProtocolCode: return "Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group."
        case InvalidGroupIdCode: return "Returned in join group when the groupId is empty or null."
        case UnknownMemberIdCode: return "Returned from group requests (offset commits/fetches, heartbeats, etc) when the memberId is not in the current generation."
        case InvalidSessionTimeoutCode: return "Return in join group when the requested session timeout is outside of the allowed range on the broker"
        case RebalanceInProgressCode: return "Returned in heartbeat requests when the coordinator has begun rebalancing the group. This indicates to the client that it should rejoin the group."
        case InvalidCommitOffsetSizeCode: return "This error indicates that an offset commit was rejected because of oversize metadata."
        case TopicAuthorizationFailedCode: return "Returned by the broker when the client is not authorized to access the requested topic."
        case GroupAuthorizationFailedCode: return "Returned by the broker when the client is not authorized to access a particular groupId."
        case ClusterAuthorizationFailedCode: return "Returned by the broker when the client is not authorized to use an inter-broker or administrative API."
        }
    }
    
    var retriable: Bool {
        switch self {
        case NoError: return false
        case Unknown: return false
        case OffsetOutOfRange: return false
        case InvalidMessage: return true
        case UnknownTopicOrPartition: return true
        case InvalidMessageSize: return false
        case LeaderNotAvailable: return true
        case NotLeaderForPartition: return true
        case RequestTimedOut: return true
        case BrokerNotAvailable: return false
        case ReplicaNotAvailable: return false
        case MessageSizeTooLarge: return false
        case StaleControllerEpochCode: return false
        case OffsetMetadataTooLargeCode: return false
        case GroupLoadInProgressCode: return true
        case GroupCoordinatorNotAvailableCode: return true
        case NotCoordinatorForGroupCode: return true
        case InvalidTopicCode: return false
        case RecordListTooLargeCode: return false
        case NotEnoughReplicasCode: return true
        case NotEnoughReplicasAfterAppendCode: return true
        case InvalidRequiredAcksCode: return false
        case IllegalGenerationCode: return false
        case InconsistentGroupProtocolCode: return false
        case InvalidGroupIdCode: return false
        case UnknownMemberIdCode: return false
        case InvalidSessionTimeoutCode: return false
        case RebalanceInProgressCode: return false
        case InvalidCommitOffsetSizeCode: return false
        case TopicAuthorizationFailedCode: return false
        case GroupAuthorizationFailedCode: return false
        case ClusterAuthorizationFailedCode: return false
        }
    }
}


enum RequestAcknowledgement {
    case NoResponse
    case Wait
    case Block
    case BlockUntil(number: Int16)
    
    var value: Int16 {
        switch self {
        case .NoResponse:
            return 0
        case .Wait:
            return 1
        case .Block:
            return -1
        case .BlockUntil(let number):
            return number
        }
    }
}


enum CompressionCodec: Int8 {
    case None = 0
    case GZIP = 1
    case Snappy = 2
}


enum ApiKey: Int16 {
    case ProduceRequest = 0
    case FetchRequest = 1
    case OffsetRequest = 2
    case MetadataRequest = 3
    case OffsetCommitRequest = 8
    case OffsetFetchRequest = 9
    case GroupCoordinatorRequest = 10
    case JoinGroupRequest = 11
    case HeartbeatRequest = 12
    case LeaveGroupRequest = 13
    case SyncGroupRequest = 14
    case DescribeGroupsRequest = 15
    case ListGroupsRequest = 16
}


enum ApiVersion: Int16 {
    case DefaultVersion = 0
}


enum GroupProtocol {
    case Consumer
    case Custom(name: String)

    var value: String {
        switch self {
        case Consumer:
            return "consumer"
        case Custom(let name):
            return name
        }
    }
}
