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
    private var _topics: [OffsetFetchTopic]
    
    init(consumerGroup: String, topics: [String:[Int32]]) {
		var values = [OffsetFetchTopic]()
		for (key, value) in topics {
			let offsetCommitTopic = OffsetFetchTopic(topic: key, partitions: value)
			values.append(offsetCommitTopic)
		}
		_consumerGroup = consumerGroup
		_topics = values
    }
    
    required init(data: inout Data) {
        _consumerGroup = String(data: &data)
        _topics = [OffsetFetchTopic](data: &data)
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
    
    private var _topicName: TopicName
    private var _partitions: [PartitionId]
    
    init(topic: TopicName, partitions: [PartitionId]) {
        self._topicName = topic
        self._partitions = partitions
    }
    
    required init(data: inout Data) {
        _topicName = String(data: &data)
        _partitions = [PartitionId](data: &data)
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


class OffsetFetchResponse: KafkaResponse {
    
    required init(data: inout Data) {
        topics = [OffsetFetchTopicResponse](data: &data)
    }
    
    private(set) var topics: [OffsetFetchTopicResponse]
    
    lazy var dataLength: Int = {
        return self.topics.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self.topics.data)
        return data
    }()
}


class OffsetFetchTopicResponse: KafkaType {
    var topic: TopicName
    
    private(set) var partitions: [OffsetFetchPartitionOffset]
    
    required init(data: inout Data) {
        topic = String(data: &data)
        partitions = [OffsetFetchPartitionOffset](data: &data)
    }
    
    lazy var dataLength: Int = {
        return self.topic.dataLength + self.partitions.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self.topic.data)
        data.append(self.partitions.data)
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
