//
//  FetchAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

class FetchRequest: KafkaRequest {
    
    var minBytes: Int32 {
        return (message as! FetchRequestMessage).minBytes
    }

    convenience init(
        topic: String,
        partition: Int32,
        offset: Int64 = 0,
        replicaId: ReplicaId = .none,
        minBytes: MinBytes = .one,
        maxWaitTime: Int32 = 500
    ) {
        let message = FetchRequestMessage(
            partitions: [topic: [partition: offset]],
            replicaId: replicaId,
            minBytes: minBytes,
            maxWaitTime: maxWaitTime
        )
        
        self.init(value: message)
    }
    
    convenience init(
        topic: String,
        partitions: [Int32] = [0],
        replicaId: ReplicaId = .none,
        minBytes: MinBytes = .one,
        maxWaitTime: Int32 = 500
    ) {
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
    
    convenience init(
        topics: [String],
        replicaId: ReplicaId = .none,
        minBytes: MinBytes = .one,
        maxWaitTime: Int32 = 500
    ) {
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
    
    convenience init(
        topics: [TopicName: [PartitionId: Offset]],
        replicaId: ReplicaId = .none,
        minBytes: MinBytes = .one,
        maxWaitTime: Int32 = 500
    ) {
        let message = FetchRequestMessage(
            partitions: topics,
            replicaId: replicaId,
            minBytes: minBytes,
            maxWaitTime: maxWaitTime
        )
        
        self.init(value: message)
    }
    
    init(value: FetchRequestMessage) {
        super.init(apiKey: ApiKey.fetchRequest, value: value)
    }

}

class FetchRequestMessage: KafkaType {

    private var _replicaId: Int32
    private var _maxWaitTime: Int32
    private var _minBytes: Int32
    private var _topics: KafkaArray<TopicalFetchMessage>

    var replicaId: Int32 {
        return _replicaId
    }
    
    var minBytes: Int32 {
        return _minBytes
    }
	
	var topics: [TopicalFetchMessage] {
		return _topics.values
	}
    
    init(
        partitions: [TopicName: [PartitionId: Offset]],
        replicaId: ReplicaId = .debug,
        minBytes: MinBytes = .one,
        maxWaitTime: Int32 = 500
    ) {
        var tempTopics = [TopicalFetchMessage]()
        
        for (topic, ps) in partitions {
            tempTopics.append(TopicalFetchMessage(value: topic, partitions: ps))
        }
        
        _topics = KafkaArray(tempTopics)
        _replicaId = replicaId.value
        _minBytes = minBytes.value
        _maxWaitTime = Int32(maxWaitTime)
    }
    
	required init(data: inout Data) {
        _replicaId = Int32(data: &data)
        _maxWaitTime = Int32(data: &data)
        _minBytes = Int32(data: &data)
        _topics = KafkaArray(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._replicaId.dataLength +
            self._maxWaitTime.dataLength +
            self._minBytes.dataLength +
            self._topics.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._replicaId.data)
        data.append(self._maxWaitTime.data)
        data.append(self._minBytes.data)
        data.append(self._topics.data)
        
        //print(self.description)
        return data
    }()
}

class TopicalFetchMessage: KafkaType {
    private var _topicName: String
    private var _partitions: KafkaArray<PartitionedFetchMessage>

    var topicName: TopicName {
        return _topicName
    }
	
	var partitions: [PartitionedFetchMessage] {
		return _partitions.values
	}
    
    init(
        value: String,
        partitions: [PartitionId: Offset]
    ) {
        _topicName = value
        var tempPartitions = [PartitionedFetchMessage]()
        for (partition, offset) in partitions {
            //print("PARTITION(\(partition)), OFFSET(\(offset))")
            tempPartitions.append(
                PartitionedFetchMessage(value: partition, offset: offset)
            )
        }
        _partitions = KafkaArray(tempPartitions)
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

class PartitionedFetchMessage: KafkaType {
    private var _partition: Int32
    private var _fetchOffset: Int64
    private var _maxBytes: Int32 = 6400
    
    var partition: PartitionId {
        return _partition
    }
    
    var offset: Offset {
        return _fetchOffset
    }
    
    var maxBytes: Int32 {
        return _maxBytes
    }
    
    init(value: Int32, offset: Int64 = 0) {
        _partition = value
        _fetchOffset = offset
    }

	required init(data: inout Data) {
        _partition = Int32(data: &data)
        _fetchOffset = Int64(data: &data)
        _maxBytes = Int32(data: &data)
    }

    lazy var dataLength: Int = {
        return self._partition.dataLength +
            self._fetchOffset.dataLength +
            self._maxBytes.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._partition.data)
        data.append(self._fetchOffset.data)
        data.append(self._maxBytes.data)
        return data
    }()
	
}


class FetchResponse: KafkaResponse {
	
	var data: Data {
		return _topics.data
	}
	
	var dataLength: Int {
		return _topics.dataLength
	}
	
    
    private var _topics: KafkaArray<TopicalFetchResponse>
    
	required init(data: inout Data) {
        _topics = KafkaArray(data: &data)
    }
	
    var topics: [TopicalFetchResponse] {
        return _topics.values
    }
}


class TopicalFetchResponse: KafkaType {
    private var _topicName: String
    private var _partitions: KafkaArray<PartitionedFetchResponse>
    
    var topicName: TopicName {
		return _topicName 
    }
    
    var partitions: [PartitionedFetchResponse] {
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


class PartitionedFetchResponse: KafkaType {

    private var _partition: Int32
    private var _errorCode: Int16
    private var _highwaterMarkOffset: Int64
    private var _messageSetSize: Int32
    private var _messageSet: MessageSet
    
    var partition: PartitionId {
        return _partition
    }
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode)
    }
    
    var offset: Int64 {
        return _highwaterMarkOffset
    }
    
    var messages: [Message] {
		return _messageSet.values.map { $0.message }
    }
	
	required init(data: inout Data) {
        _partition = Int32(data: &data)
        _errorCode = Int16(data: &data)
        _highwaterMarkOffset = Offset(data: &data)
        _messageSetSize = Int32(data: &data)
		var messageSetData = data.take(first: Int(_messageSetSize))
        _messageSet = MessageSet(data: &messageSetData)
    }
    
    lazy var dataLength: Int = {
        return self._partition.dataLength +
            self._errorCode.dataLength +
            self._highwaterMarkOffset.dataLength +
            self._messageSetSize.dataLength +
            self._messageSet.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._partition.data)
        data.append(self._errorCode.data)
        data.append(self._highwaterMarkOffset.data)
        data.append(self._messageSetSize.data)
        data.append(self._messageSet.data)
        return data
    }()
}
