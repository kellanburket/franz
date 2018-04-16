//
//  OffsetCommitFetchAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

typealias OffsetMetadata = String

struct OffsetCommitRequest: KafkaRequest {
	
	typealias Response = OffsetCommitResponse
	
	var apiKey: ApiKey { return .offsetCommitRequest }
	var apiVersion: ApiVersion { return .v2 }
	
    init(
        consumerGroupId: String,
        generationId: Int32,
        consumerId: String,
        topics: [TopicName: [PartitionId: (Offset, OffsetMetadata?)]],
        retentionTime: Int64 = 0
    ) {
        self.init(
            value: OffsetCommitRequestMessage(
                consumerGroupId: consumerGroupId,
                generationId: generationId,
                consumerId: consumerId,
                topics: topics,
                retentionTime: retentionTime
            )
        )
    }
	
	let value: KafkaType?
    init(value: OffsetCommitRequestMessage) {
		self.value = value
    }
}

struct OffsetCommitRequestMessage: KafkaType {

    let consumerGroupId: String
    let consumerGroupGenerationId: Int32
    let consumerId: String
    let retentionTime: Int64
    let topics: [OffsetCommitTopic]
 
    init(consumerGroupId: String, generationId: Int32, consumerId: String, topics: [TopicName: [PartitionId: (Offset, OffsetMetadata?)]], retentionTime: Int64 = 0) {
		self.consumerGroupId = consumerGroupId
		self.consumerGroupGenerationId = generationId
		self.consumerId = consumerId
		self.retentionTime = retentionTime
		self.topics = [OffsetCommitTopic](topics.map { arg in
			let (key, value) = arg
			return OffsetCommitTopic(topic: key, partitions: value)
		})
    }
    
    init(data: inout Data) {
        consumerGroupId = String(data: &data)
        consumerGroupGenerationId = Int32(data: &data)
        consumerId = String(data: &data)
        retentionTime = Int64(data: &data)
        topics = [OffsetCommitTopic](data: &data)
    }

    var dataLength: Int {
		let values: [KafkaType] = [consumerGroupId, consumerGroupGenerationId, consumerId, retentionTime, topics]
		return values.map { $0.dataLength }.reduce(0, +)
	}
    
    var data: Data {
        var data = Data(capacity: dataLength)
        data += consumerGroupId.data
        data += consumerGroupGenerationId.data
        data += consumerId.data
        data += retentionTime.data
        data += topics.data
        return data
    }
}


struct OffsetCommitTopic: KafkaType {
	let topicName: TopicName
    let partitions: [OffsetCommitPartitionOffset]

    init(topic: TopicName, partitions: [PartitionId: (Offset, OffsetMetadata?)]) {
        self.topicName = topic
		self.partitions = partitions.map { arg in
			let (key, value) = arg
			return OffsetCommitPartitionOffset(partition: key, offset: value.0, metadata: value.1)
		}
    }
    
	init(data: inout Data) {
        topicName = String(data: &data)
        partitions = [OffsetCommitPartitionOffset](data: &data)
    }

    var dataLength: Int {
        return topicName.dataLength + partitions.dataLength
	}
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data += topicName.data
        data += partitions.data
        return data
	}
}

struct OffsetCommitPartitionOffset: KafkaType {
    let partition: PartitionId
    let offset: Offset
    let metadata: String?

    init(partition: PartitionId, offset: Offset, metadata: String? = nil) {
        self.partition = partition
		self.offset = offset
        self.metadata = metadata
    }

    init(data: inout Data) {
        partition = PartitionId(data: &data)
        offset = Offset(data: &data)
        metadata = String(data: &data)
    }

    var dataLength: Int {
        return partition.dataLength + offset.dataLength + metadata.dataLength
	}
	
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data += partition.data
        data += offset.data
		data += metadata.data
        return data
	}
}


struct OffsetCommitResponse: KafkaResponse {
    
    let topics: [OffsetCommitTopicResponse]
    
    init(data: inout Data) {
        topics = [OffsetCommitTopicResponse](data: &data)
    }
    
    var dataLength: Int {
        return topics.dataLength
	}
	
    var data: Data {
        return topics.data
	}
}

struct OffsetCommitTopicResponse: KafkaType {
    
    let topicName: String
    let partitions: [OffsetCommitPartitionResponse]
    
    init(data: inout Data) {
        topicName = String(data: &data)
        partitions = [OffsetCommitPartitionResponse](data: &data)
    }
    
    var dataLength: Int {
        return topicName.dataLength + partitions.dataLength
	}
		
	var data: Data {
        return topicName.data + partitions.data
	}
}

struct OffsetCommitPartitionResponse: KafkaType {
    
    let partition: Int32
    private var errorCode: Int16
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: errorCode)
    }
    
    init(data: inout Data) {
        partition = Int32(data: &data)
        errorCode = Int16(data: &data)
    }
    
    var dataLength: Int {
        return partition.dataLength + errorCode.dataLength
	}
    
    var data: Data {
		return partition.data + errorCode.data
	}
	
}



