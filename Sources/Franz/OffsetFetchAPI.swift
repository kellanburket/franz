//
//  OffsetFetchAPI.swift
//  Pods
//
//  Created by Kellan Cummings on 1/31/16.
//
//

import Foundation


class OffsetFetchRequest: KafkaRequest {
    convenience init(
        consumerGroupId: String,
        topics: [String: [Int32]]
    ) {
        self.init(
            value: OffsetFetchRequestMessage(
                consumerGroup: consumerGroupId,
                topics: topics
            )
        )
    }
    
    init(value: OffsetFetchRequestMessage) {
        super.init(apiKey: ApiKey.offsetFetchRequest, value: value)
    }
}


class OffsetFetchRequestMessage: KafkaType {
    
    private var _consumerGroup: String
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
            _consumerGroup = consumerGroup
            _topics = KafkaArray(values)
    }
    
    required init(data: inout Data) {
        _consumerGroup = String(data: &data)
        _topics = KafkaArray(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._consumerGroup.dataLength +
            self._topics.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._consumerGroup.data)
        data.append(self._topics.data)
        return data
    }()
}

class OffsetFetchTopic: KafkaType {
    
    private var _topicName: String
    private var _partitions: KafkaArray<Int32>
    
    init(
        topic: String,
        partitions: [Int32]
        ) {
            _topicName = topic
            var values = [Int32]()
            
            for partition in partitions {
                values.append(partition)
            }
            
            _partitions = KafkaArray(values)
    }
    
    required init(data: inout Data) {
        _topicName = String(data: &data)
        _partitions = KafkaArray(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._topicName.dataLength +
            self._partitions.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._topicName.data)
        data.append(self._partitions.data)
        return data
    }()
}


class OffsetFetchResponse: KafkaResponse {
    
    private var _topics: KafkaArray<OffsetFetchTopicResponse>
    
    required init(data: inout Data) {
        _topics = KafkaArray(data: &data)
    }
    
    var topics: [OffsetFetchTopicResponse] {
        return _topics.values
    }
    
    lazy var dataLength: Int = {
        return self._topics.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._topics.data)
        return data
    }()
}


class OffsetFetchTopicResponse: KafkaType {
    private var _topicName: String
    private var _partitions: KafkaArray<OffsetFetchPartitionOffset>
    
    var topic: String {
        return _topicName
    }
    
    var partitions: [OffsetFetchPartitionOffset] {
        return _partitions.values
    }
    
    required init(data: inout Data) {
        _topicName = String(data: &data)
        _partitions = KafkaArray(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._topicName.dataLength + self._partitions.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._topicName.data)
        data.append(self._partitions.data)
        return data
    }()
}

typealias Offset = Int64

class OffsetFetchPartitionOffset: KafkaType {
    private var _partition: Int32
    private var _offset: Offset
    private var _metadata: String
    private var _errorCode: Int16
    
    var error: KafkaErrorCode? {
        if _offset == -1 {
            return KafkaErrorCode.noError
        } else {
            return KafkaErrorCode(rawValue: _errorCode)
        }
    }
    
    var partition: Int32 {
        return _partition
    }
    
    var metadata: String {
        return _metadata
    }
    
    var offset: Offset {
        return _offset == -1 ? 0 : _offset
    }
    
    required init(data: inout Data) {
        _partition = Int32(data: &data)
        _offset = Int64(data: &data)
        _metadata = String(data: &data)
        _errorCode = Int16(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._partition.dataLength +
            self._offset.dataLength +
            self._metadata.dataLength +
            self._errorCode.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._partition.data)
        data.append(self._offset.data)
        data.append(self._metadata.data)
        data.append(self._errorCode.data)
        return data
    }()
}
