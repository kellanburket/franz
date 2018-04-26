//
//  FetchAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

struct FetchRequest: KafkaRequest {
	static let apiVersion: ApiVersion = 0
	
	static let apiKey: ApiKey = .fetchRequest
	
    var minBytes: Int32 {
        return (values[0] as! FetchRequestMessage).minBytes
    }

	init(topic: String,
		 partition: Int32,
		 offset: Int64 = 0,
		 replicaId: ReplicaId = .none,
		 minBytes: MinBytes = .one,
		 maxWaitTime: Int32 = 500) {
		let message = FetchRequestMessage(
			partitions: [topic: [partition: offset]],
			replicaId: replicaId,
			minBytes: minBytes,
			maxWaitTime: maxWaitTime)
        
        self.init(value: message)
    }
    
	init(topic: String,
		 partitions: [Int32] = [0],
		 replicaId: ReplicaId = .none,
		 minBytes: MinBytes = .one,
		 maxWaitTime: Int32 = 500) {
        var tempPartitions = [Int32:Int64]()
        
        for partition in partitions {
            tempPartitions[partition] = 0
        }

        let message = FetchRequestMessage(
            partitions: [topic: tempPartitions],
            replicaId: replicaId,
            minBytes: minBytes,
            maxWaitTime: maxWaitTime
        )
        
        self.init(value: message)
    }
	
	init(topics: [String],
		 replicaId: ReplicaId = .none,
		 minBytes: MinBytes = .one,
		 maxWaitTime: Int32 = 500) {
        var partitions = [String:[Int32:Int64]]()
        
        for topic in topics {
            partitions[topic] = [0:0]
        }

        let message = FetchRequestMessage(
            partitions: partitions,
            replicaId: replicaId,
            minBytes: minBytes,
            maxWaitTime: maxWaitTime
        )
        
        self.init(value: message)
    }
    
	init(topics: [TopicName: [PartitionId: Offset]],
		 replicaId: ReplicaId = .none,
		 minBytes: MinBytes = .one,
		 maxWaitTime: Int32 = 500) {
        let message = FetchRequestMessage(
            partitions: topics,
            replicaId: replicaId,
            minBytes: minBytes,
            maxWaitTime: maxWaitTime
        )
        
        self.init(value: message)
    }
	
	let values: [Encodable]
    init(value: FetchRequestMessage) {
		self.values = [value]
    }

	struct Response: Decodable {
		let topics: [TopicalFetchResponse]
	}
}

struct FetchRequestMessage: Encodable {
    let replicaId: Int32
    private let maxWaitTime: Int32
    let minBytes: Int32
	private let topics: [TopicalFetchMessage]
    
    init(partitions: [TopicName: [PartitionId: Offset]],
        replicaId: ReplicaId = .debug,
        minBytes: MinBytes = .one,
        maxWaitTime: Int32 = 500) {
        var tempTopics = [TopicalFetchMessage]()
        
        for (topic, ps) in partitions {
            tempTopics.append(TopicalFetchMessage(value: topic, partitions: ps))
        }
        
        self.topics = tempTopics
        self.replicaId = replicaId.value
        self.minBytes = minBytes.value
        self.maxWaitTime = Int32(maxWaitTime)
    }
}

struct TopicalFetchMessage: Encodable {
	let topicName: String
	let partitions: [PartitionedFetchMessage]
    
    init(value: String, partitions: [PartitionId: Offset]) {
        topicName = value
        var tempPartitions = [PartitionedFetchMessage]()
        for (partition, offset) in partitions {
            tempPartitions.append(PartitionedFetchMessage(value: partition, offset: offset))
        }
        self.partitions = tempPartitions
    }
}

struct PartitionedFetchMessage: Encodable {
	let partition: PartitionId
    let offset: Offset
    let maxBytes: Int32 = 6400
    
    init(value: Int32, offset: Int64 = 0) {
        self.partition = value
        self.offset = offset
    }
}

struct TopicalFetchResponse: Decodable {
    let topicName: TopicName
    let partitions: [PartitionedFetchResponse]
}


struct PartitionedFetchResponse: Decodable {
    let partition: PartitionId
    let errorCode: KafkaErrorCode
    let offset: Offset
    private let messageSetSize: Int32
    private let messageSet: MessageSet
    
    var messages: [Message] {
		return messageSet.values.map { $0.message }
    }
	
	init(from decoder: Decoder) throws {
		partition = try PartitionId(from: decoder)
		errorCode = try KafkaErrorCode(from: decoder)
		offset = try Offset(from: decoder)
		messageSetSize = try Int32(from: decoder)
		var messageSetData = data.take(first: Int(messageSetSize))
		messageSet = MessageSet(data: &messageSetData)
	}
}
