//
//  Enumerations.swift
//  Franz
//
//  Created by Kellan Cummings on 1/21/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

/*
    Used to ask for all messages before a certain time (ms). Pulled in descending order.
*/
enum TimeOffset {
    case latest  /* the latest offset (i.e. the offset of the next coming message) */
    case earliest  /* earliest available offset; will always return a single element */
    case preceding(ms: Int64)
    
    var value: Int64 {
        switch self {
        case .latest:
            return -1
        case .earliest:
            return -2
        case .preceding(let ms):
            return ms
        }
    }
}


/**
    The minimum number of bytes of messages that must be available to give a response.
*/
enum MinBytes {
    case none
    case one
    case many(bytes: Int32)
    
    var value: Int32 {
        switch self {
        case .none:
            return 0
        case .one:
            return 1
        case .many(let bytes):
            return bytes
        }
    }
}


enum ReplicaId {
    case none
    case node(id: Int32)
    case debug
    
    var value: Int32 {
        switch self {
        case .none:
            return -1
        case .debug:
            return -2
        case .node(let id):
            return id
        }
    }
}

/*
    Built in Kafka error codes
*/
enum KafkaErrorCode: Int16 {
    case noError = 0
    case unknown = -1
    case offsetOutOfRange = 1
    case invalidMessage = 2
    case unknownTopicOrPartition = 3
    case invalidMessageSize = 4
    case leaderNotAvailable = 5
    case notLeaderForPartition = 6
    case requestTimedOut = 7
    case brokerNotAvailable = 8
    case replicaNotAvailable = 9
    case messageSizeTooLarge = 10
    case staleControllerEpochCode = 11
    case offsetMetadataTooLargeCode = 12
    case groupLoadInProgressCode = 14
    case groupCoordinatorNotAvailableCode = 15
    case notCoordinatorForGroupCode = 16
    case invalidTopicCode = 17
    case recordListTooLargeCode = 18
    case notEnoughReplicasCode = 19
    case notEnoughReplicasAfterAppendCode = 20
    case invalidRequiredAcksCode = 21
    case illegalGenerationCode = 22
    case inconsistentGroupProtocolCode = 23
    case invalidGroupIdCode = 24
    case unknownMemberIdCode = 25
    case invalidSessionTimeoutCode = 26
    case rebalanceInProgressCode = 27
    case invalidCommitOffsetSizeCode = 28
    case topicAuthorizationFailedCode = 29
    case groupAuthorizationFailedCode = 30
    case clusterAuthorizationFailedCode = 31
    
    var code: Int16 {
        return self.rawValue
    }
    
    var description: String {
        switch self {
        case .noError:
            return "No error."
        case .unknown:
            return "An unexpected server error"
        case .offsetOutOfRange:
            return "The requested offset is outside the range of offsets maintained" +
                " by the server for the given topic/partition."
        case .invalidMessage:
            return "Message contents does not match its CRC"
        case .unknownTopicOrPartition:
            return "Topic or partition does not exist on this broker."
        case .invalidMessageSize:
            return "Message has a negative size"
        case .leaderNotAvailable:
            return "This error is thrown if we are in the middle of a leadership election" +
                " and there is currently no leader for this partition and hence it is unavailable for writes."
        case .notLeaderForPartition:
            return "This error is thrown if the client attempts to send messages to a " +
                " replica that is not the leader for some partition. It indicates that the clients metadata is out of date."
        case .requestTimedOut:
            return "This error is thrown if the request exceeds the user-specified time limit in the request."
        case .brokerNotAvailable:
            return "This is not a client facing error and is used mostly by tools when a broker is not alive."
        case .replicaNotAvailable:
            return "If replica is expected on a broker, but is not (this can be safely ignored)."
        case .messageSizeTooLarge:
            return "The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum."
        case .staleControllerEpochCode:
            return "Internal error."
        case .offsetMetadataTooLargeCode:
            return "String larger than configured maximum for offset metadata"
        case .groupLoadInProgressCode:
            return "The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator."
        case .groupCoordinatorNotAvailableCode:
            return "Offsets topic has not yet been created or group coordinator is not active."
        case .notCoordinatorForGroupCode:
            return "Broke is not group coordinator for offset fetch or commit request."
        case .invalidTopicCode:
            return "For a request which attempts to access an invalid topic (e.g. one which has an illegal name), or if an attempt is made to write to an internal topic (such as the consumer offsets topic)."
        case .recordListTooLargeCode:
            return "Request exceeds the maximum configured segment size."
        case .notEnoughReplicasCode:
            return "The number of in-sync replicas is lower than the configured minimum and requiredAcks is -1."
        case .notEnoughReplicasAfterAppendCode:
            return "Message was written to the log, but with fewer in-sync replicas than required."
        case .invalidRequiredAcksCode:
            return "RequiredAcks is invalid (anything other than -1, 1, or 0)."
        case .illegalGenerationCode:
            return "Generation id provided in the request is not the current generation."
        case .inconsistentGroupProtocolCode:
            return "Protocol type or set of protocols is not compatible with the current group."
        case .invalidGroupIdCode:
            return "The groupId is empty or null."
        case .unknownMemberIdCode:
            return "The memberId is not in the current generation."
        case .invalidSessionTimeoutCode:
            return "The requested session timeout is outside of the allowed range on the broker"
        case .rebalanceInProgressCode:
            return "Client that it should rejoin the group."
        case .invalidCommitOffsetSizeCode:
            return "Offset commit rejected due to oversize metadata."
        case .topicAuthorizationFailedCode:
            return "The client is not authorized to access the requested topic."
        case .groupAuthorizationFailedCode:
            return "The client is not authorized to access a particular groupId."
        case .clusterAuthorizationFailedCode:
            return "The client is not authorized to use an inter-broker or administrative API."
        }
    }
    
    var retriable: Bool {
        switch self {
        case .noError: return false
        case .unknown: return false
        case .offsetOutOfRange: return false
        case .invalidMessage: return true
        case .unknownTopicOrPartition: return true
        case .invalidMessageSize: return false
        case .leaderNotAvailable: return true
        case .notLeaderForPartition: return true
        case .requestTimedOut: return true
        case .brokerNotAvailable: return false
        case .replicaNotAvailable: return false
        case .messageSizeTooLarge: return false
        case .staleControllerEpochCode: return false
        case .offsetMetadataTooLargeCode: return false
        case .groupLoadInProgressCode: return true
        case .groupCoordinatorNotAvailableCode: return true
        case .notCoordinatorForGroupCode: return true
        case .invalidTopicCode: return false
        case .recordListTooLargeCode: return false
        case .notEnoughReplicasCode: return true
        case .notEnoughReplicasAfterAppendCode: return true
        case .invalidRequiredAcksCode: return false
        case .illegalGenerationCode: return false
        case .inconsistentGroupProtocolCode: return false
        case .invalidGroupIdCode: return false
        case .unknownMemberIdCode: return false
        case .invalidSessionTimeoutCode: return false
        case .rebalanceInProgressCode: return false
        case .invalidCommitOffsetSizeCode: return false
        case .topicAuthorizationFailedCode: return false
        case .groupAuthorizationFailedCode: return false
        case .clusterAuthorizationFailedCode: return false
        }
    }
}


enum RequestAcknowledgement {
    case noResponse
    case wait
    case block
    case blockUntil(number: Int16)
    
    var value: Int16 {
        switch self {
        case .noResponse:
            return 0
        case .wait:
            return 1
        case .block:
            return -1
        case .blockUntil(let number):
            return number
        }
    }
}


enum CompressionCodec: Int8 {
    case none = 0
    case gzip = 1
    case snappy = 2
}


enum ApiKey: Int16 {
    case produceRequest = 0
    case fetchRequest = 1
    case offsetRequest = 2
    case metadataRequest = 3
    case offsetCommitRequest = 8
    case offsetFetchRequest = 9
    case groupCoordinatorRequest = 10
    case joinGroupRequest = 11
    case heartbeatRequest = 12
    case leaveGroupRequest = 13
    case syncGroupRequest = 14
    case describeGroupsRequest = 15
    case listGroupsRequest = 16
}


enum ApiVersion: Int16 {
	static var defaultVersion: ApiVersion {
		return v0
	}
	case v0 = 0
	case v1 = 1
	case v2 = 2
}


/**
    Group Protocols
**/
public enum GroupProtocol {
    case consumer
    case custom(name: String)

    var value: String {
        switch self {
        case .consumer:
            return "consumer"
        case .custom(let name):
            return name
        }
    }
}

/**
    Group States
*/
public enum GroupState: String {
    /**
        Returned when the join group phase has completed (i.e. all expected members 
        of the group have sent JoinGroup requests) and the coordinator is awaiting 
        group state from the leader. Unexpected coordinator requests return an error 
        indicating that a rebalance is in progress.
    */
    case AwaitingSync = "AwaitingSync"
    /**
        Returned when The coordinator is reading group data from Zookeeper (or some 
        other storage) in order to transition groups from failed coordinators. Any 
        heartbeat or join group requests are returned with an error indicating that 
        the coordinator is not ready yet.
    */
    case Initialize = "Initialize"
    /**
        Returned when the coordinator has received a JoinGroup request from at least 
        one member and is awaiting JoinGroup requests from the rest of the group. 
        Heartbeats or SyncGroup requests in this state return an error indicating 
        that a rebalance is in progress.
    */
    case Joining = "Joining"
    /**
        Returned when The coordinator either has an active generation or has no members 
        and is awaiting the first JoinGroup. Heartbeats are accepted from members in 
        this state and are used to keep group members active or to indicate that they 
        need to join the group.
    */
    case Stable = "Stable"
    /**
        Returned when there are no active members and group state has been cleaned up.
    */
    case Down = "Down"
}

/**
    Consumer Assignment Strategy
*/
public enum AssignmentStrategy: String {
    case Range = "_"
    case RoundRobin = "roundrobin"
}

