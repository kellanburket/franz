//
//  OffsetCommitFetchAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

class OffsetCommitRequest: KafkaRequest {
    
    convenience init(
        consumerGroupId: String,
        generationId: Int32,
        consumerId: String,
        topics: [String:[Int32: (Int64, String)]],
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
        super.init(apiKey: ApiKey.OffsetCommitRequest, value: value)
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
        topics: [String:[Int32: (Int64, String)]],
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
    
    required init(inout bytes: [UInt8]) {
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
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._consumerGroupId.data)
        data.appendData(self._consumerGroupGenerationId.data)
        data.appendData(self._consumerId.data)
        data.appendData(self._retentionTime.data)
        data.appendData(self._topics.data)
        return data
    }()
    
    lazy var description: String = {
        return "OFFSET COMMIT REQUEST:\n" +
            "\tCONSUMER GROUP ID: \(self._consumerGroupId.value)" +
            "\tCONSUMER GROUP GENERATION ID: \(self._consumerGroupGenerationId.value)" +
            "\tCONSUMER ID ID: \(self._consumerId.value)" +
            "\tRETENTION TIME ID: \(self._retentionTime.value)" +
            "\tTOPICS:\n" +
            self._topics.description
    }()
}


class OffsetCommitTopic: KafkaClass {
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<OffsetCommitPartitionOffset>

    init(topic: String, partitions: [Int32:(Int64, String)]) {
        _topicName = KafkaString(value: topic)
        var values = [OffsetCommitPartitionOffset]()
        for (key, value) in partitions {
            values.append(OffsetCommitPartitionOffset(partition: key, offset: value.0, metadata: value.1))
        }
        _partitions = KafkaArray(values: values)
    }
    
    required init(inout bytes: [UInt8]) {
        _topicName = KafkaString(bytes: &bytes)
        _partitions = KafkaArray(bytes: &bytes)
    }

    lazy var length: Int = {
        return self._topicName.length + self._partitions.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._topicName.data)
        data.appendData(self._partitions.data)
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

    init(partition: Int32, offset: Int64, metadata: String) {
        _partition = KafkaInt32(value: partition)
        _offset = KafkaInt64(value: offset)
        _metadata = KafkaString(value: metadata)
    }

    required init(inout bytes: [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _offset = KafkaInt64(bytes: &bytes)
        _metadata = KafkaString(bytes: &bytes)
    }

    lazy var length: Int = {
        return self._partition.length +
            self._offset.length +
            self._metadata.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._partition.data)
        data.appendData(self._offset.data)
        data.appendData(self._metadata.data)
        return data
    }()
    
    lazy var description: String = {
        return ""
    }()
}


class OffsetCommitFetchResponse: KafkaResponse {
    
    private var _topics: KafkaArray<OffsetCommitTopicResponse>
    
    required init(inout bytes: [UInt8]) {
        _topics = KafkaArray(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._topics.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        return data
    }()
    
    override var description: String {
        return _topics.description
    }
}

class OffsetCommitTopicResponse: KafkaClass {
    
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<OffsetCommitPartitionResponse>
    
    required init(inout bytes: [UInt8]) {
        _topicName = KafkaString(bytes: &bytes)
        _partitions = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._topicName.length + self._partitions.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        return data
    }()
    
    lazy var description: String = {
        return "\tTOPIC: \(self._topicName.value)" +
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
    
    required init(inout bytes: [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _errorCode = KafkaInt16(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._partition.length + self._errorCode.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        return data
    }()
    
    lazy var description: String = {
        return "\t\tPARTITION: \(self.partition)\n" +
            "\t\tERROR: \(self.error?.description ?? String())"
    }()
}


class OffsetFetchRequest: KafkaRequest {

    convenience init(
        group: String,
        topics: [String: [Int32]]
    ) {
        self.init(
            value: OffsetFetchRequestMessage(
                consumerGroup: group,
                topics: topics
            )
        )
    }
    
    init(value: OffsetFetchRequestMessage) {
        super.init(apiKey: ApiKey.OffsetFetchRequest, value: value)
    }
}


class OffsetFetchRequestMessage: KafkaClass {
    
    private var _consumerGroup: KafkaString
    private var _topics: KafkaArray<OffsetFetchTopic>
    
    init(
        consumerGroup: String,
        topics: [String:[Int32]]
    ) {
        var values = [OffsetFetchTopic]()
        for (key, value) in topics {
            let offsetCommitTopic = OffsetFetchTopic(topic: key, partitions: value)
            values.append(offsetCommitTopic)
        }
        _consumerGroup = KafkaString(value: consumerGroup)
        _topics = KafkaArray(values: values)
    }
    
    required init(inout bytes: [UInt8]) {
        _consumerGroup = KafkaString(bytes: &bytes)
        _topics = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._consumerGroup.length +
            self._topics.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._consumerGroup.data)
        data.appendData(self._topics.data)
        return data
    }()
    
    lazy var description: String = {
        return "OFFSET COMMIT REQUEST:\n" +
            "\tCONSUMER GROUP: \(self._consumerGroup.value)" +
            "\tTOPICS:\n" +
            self._topics.description
    }()
}

class OffsetFetchTopic: KafkaClass {
    
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<KafkaInt32>
    
    init(
        topic: String,
        partitions: [Int32]
    ) {
        _topicName = KafkaString(value: topic)
        var values = [KafkaInt32]()

        for partition in partitions {
            values.append(KafkaInt32(value: partition))
        }
        
        _partitions = KafkaArray(values: values)
    }
    
    required init(inout bytes: [UInt8]) {
        _topicName = KafkaString(bytes: &bytes)
        _partitions = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._topicName.length +
            self._partitions.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._topicName.data)
        data.appendData(self._partitions.data)
        return data
    }()
    
    lazy var description: String = {
        return "\tTOPIC: \(self._topicName.value)" +
            "\tPARTITIONS:\n" +
            self._partitions.description
    }()
}


class OffsetFetchResponse: KafkaResponse {
    
    private var _topics: KafkaArray<OffsetFetchTopicResponse>
    
    required init(inout bytes: [UInt8]) {
        _topics = KafkaArray(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._topics.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._topics.data)
        return data
    }()
    
    override var description: String {
        return "OFFSET FETCH REQUEST:\n" +
            "\tTOPICS:\n" +
            self._topics.description
    }
}


class OffsetFetchTopicResponse: KafkaClass {
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<OffsetFetchPartitionOffset>
    
    var topic: String {
        return _topicName.value
    }
    
    required init(inout bytes: [UInt8]) {
        _topicName = KafkaString(bytes: &bytes)
        _partitions = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._topicName.length + self._partitions.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._topicName.data)
        data.appendData(self._partitions.data)
        return data
    }()
    
    lazy var description: String = {
        return "\t\tNAME: \(self.topic)\n" +
            "\t\tPARTITIONS:\n" +
            self._partitions.description
    }()
}


class OffsetFetchPartitionOffset: KafkaClass {
    private var _partition: KafkaInt32
    private var _offset: KafkaInt64
    private var _metadata: KafkaString
    private var _errorCode: KafkaInt16
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode.value)
    }
    
    var partition: Int32 {
        return _partition.value
    }
    
    var metadata: String {
        return _metadata.value
    }
    
    var offset: Int64 {
        return _offset.value
    }
    
    required init(inout bytes: [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _offset = KafkaInt64(bytes: &bytes)
        _metadata = KafkaString(bytes: &bytes)
        _errorCode = KafkaInt16(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._partition.length +
            self._offset.length +
            self._metadata.length +
            self._errorCode.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._partition.data)
        data.appendData(self._offset.data)
        data.appendData(self._metadata.data)
        data.appendData(self._errorCode.data)
        return data
    }()
    
    lazy var description: String = {
        return "\t\t\tPARTITION: \(self.partition)\n" +
            "\t\t\tOFFSET: \(self.offset)\n" +
            "\t\t\tMETADATA: \(self.metadata)\n" +
            "\t\t\tERROR: \(self.error?.description ?? String())"
    }()
}



