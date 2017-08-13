//
//  OffsetCommitFetchAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


class OffsetCommitRequest: KafkaRequest {
	
	typealias Metadata = String
    
    convenience init(
        consumerGroupId: String,
        generationId: Int32,
        consumerId: String,
        topics: [TopicName: [PartitionId: (Offset, Metadata?)]],
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
    
    init(value: OffsetCommitRequestMessage) {
		super.init(apiKey: ApiKey.offsetCommitRequest, value: value, apiVersion: .v2)
    }
}

class OffsetCommitRequestMessage: KafkaClass {

    private var _consumerGroupId: KafkaString
    private var _consumerGroupGenerationId: KafkaInt32
    private var _consumerId: KafkaString
    private var _retentionTime: KafkaInt64
    private var _topics: KafkaArray<OffsetCommitTopic>
 
    init(
        consumerGroupId: String,
        generationId: Int32,
        consumerId: String,
        topics: [String:[Int32: (Int64, String?)]],
        retentionTime: Int64 = 0
    ) {
        var values = [OffsetCommitTopic]()
        for (key, value) in topics {
            let offsetCommitTopic = OffsetCommitTopic(topic: key, partitions: value)
            values.append(offsetCommitTopic)
        }
        _consumerGroupId = KafkaString(value: consumerGroupId)
        _consumerGroupGenerationId = KafkaInt32(value: generationId)
        _consumerId = KafkaString(value: consumerId)
        _retentionTime = KafkaInt64(value: retentionTime)
        _topics = KafkaArray(values: values)
    }
    
    required init(bytes: inout [UInt8]) {
        _consumerGroupId = KafkaString(bytes: &bytes)
        _consumerGroupGenerationId = KafkaInt32(bytes: &bytes)
        _consumerId = KafkaString(bytes: &bytes)
        _retentionTime = KafkaInt64(bytes: &bytes)
        _topics = KafkaArray(bytes: &bytes)
    }

    lazy var length: Int = {
        return self._consumerGroupId.length +
            self._consumerGroupGenerationId.length +
            self._consumerId.length +
            self._retentionTime.length +
            self._topics.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        data.append(self._consumerGroupId.data)
        data.append(self._consumerGroupGenerationId.data)
        data.append(self._consumerId.data)
        data.append(self._retentionTime.data )
        data.append(self._topics.data)
        return data
    }()
    
    lazy var description: String = {
        return "OFFSET COMMIT REQUEST:\n" +
            "\tCONSUMER GROUP ID: \(self._consumerGroupId.value ?? "nil")\n" +
            "\tCONSUMER GROUP GENERATION ID: \(self._consumerGroupGenerationId.value)\n" +
			"\tCONSUMER ID ID: \(self._consumerId.value ?? "nil")\n" +
            "\tRETENTION TIME ID: \(self._retentionTime.value)\n" +
            "\tTOPICS:\n" +
            self._topics.description
    }()
}


class OffsetCommitTopic: KafkaClass {
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<OffsetCommitPartitionOffset>

    init(topic: String, partitions: [Int32:(Int64, String?)]) {
        _topicName = KafkaString(value: topic)
        var values = [OffsetCommitPartitionOffset]()
        for (key, value) in partitions {
            values.append(OffsetCommitPartitionOffset(partition: key, offset: value.0, metadata: value.1))
        }
        _partitions = KafkaArray(values: values)
    }
    
    required init(bytes: inout [UInt8]) {
        _topicName = KafkaString(bytes: &bytes)
        _partitions = KafkaArray(bytes: &bytes)
    }

    lazy var length: Int = {
        return self._topicName.length + self._partitions.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        data.append(self._topicName.data)
        data.append(self._partitions.data)
        return data
    }()
    
    lazy var description: String = {
        return ""
    }()
}


class OffsetCommitPartitionOffset: KafkaClass {
    private var _partition: KafkaInt32
    private var _offset: KafkaInt64
    private var _metadata: KafkaString

    init(partition: Int32, offset: Int64, metadata: String?) {
        _partition = KafkaInt32(value: partition)
        _offset = KafkaInt64(value: offset)
        _metadata = KafkaString(value: metadata)
    }

    required init(bytes: inout [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _offset = KafkaInt64(bytes: &bytes)
        _metadata = KafkaString(bytes: &bytes)
    }

    lazy var length: Int = {
        return self._partition.length +
            self._offset.length +
            self._metadata.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        data.append(self._partition.data)
        data.append(self._offset.data)
        data.append(self._metadata.data)
        return data
    }()
    
    lazy var description: String = {
        return ""
    }()
}


class OffsetCommitResponse: KafkaResponse {
    
    private var _topics: KafkaArray<OffsetCommitTopicResponse>
    
    required init(bytes: inout [UInt8]) {
        _topics = KafkaArray(bytes: &bytes)
    }
    
    var topics: [OffsetCommitTopicResponse] {
        return _topics.values
    }
    
    lazy var length: Int = {
        return self._topics.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        return data
    }()
    
    var description: String {
        return _topics.description
    }
}

class OffsetCommitTopicResponse: KafkaClass {
    
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<OffsetCommitPartitionResponse>
    
    required init(bytes: inout [UInt8]) {
        _topicName = KafkaString(bytes: &bytes)
        _partitions = KafkaArray(bytes: &bytes)
    }
    
    var partitions: [OffsetCommitPartitionResponse] {
        return _partitions.values
    }
    
    lazy var length: Int = {
        return self._topicName.length + self._partitions.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        return data
    }()
    
    lazy var description: String = {
        return "\tTOPIC: \(self._topicName.value ?? "nil")" +
            "\tPARTITIONS:\n" +
            self._partitions.description
    }()
}

class OffsetCommitPartitionResponse: KafkaClass {
    
    private var _partition: KafkaInt32
    private var _errorCode: KafkaInt16
    
    var partition: Int32 {
        return _partition.value
    }
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode.value)
    }
    
    required init(bytes: inout [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _errorCode = KafkaInt16(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._partition.length + self._errorCode.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        return data
    }()
    
    lazy var description: String = {
        return "\t\tPARTITION: \(self.partition)\n" +
            "\t\tERROR: \(self.error?.description ?? String())"
    }()
}



