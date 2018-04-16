//
//  OffsetAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

struct OffsetRequest: KafkaRequest {
	
	typealias Response = OffsetResponse
	var apiKey: ApiKey { return .offsetRequest }

    init(
		topics: [TopicName: [PartitionId]],
        time: TimeOffset = TimeOffset.latest,
        maxNumberOfOffsets: Int32 = 10,
        replicaId: ReplicaId = .none
    ) {
		let topicsWithSettings = topics.mapValues { partitions -> [PartitionId: (TimeOffset, Int32)] in
			var topicValues = [PartitionId: (TimeOffset, Int32)]()
			for partition in partitions {
				topicValues[partition] = (time, maxNumberOfOffsets)
			}
			return topicValues
		}
        
        self.init(
            value: OffsetRequestMessage(
                topics: topicsWithSettings,
                replicaId: replicaId
            )
        )
    }
    
    init(
        topics: [TopicName: [PartitionId: (TimeOffset, Int32)]],
        replicaId: ReplicaId = .none
    ) {
        self.init(
            value: OffsetRequestMessage(
                topics: topics,
                replicaId: replicaId
            )
        )
    }
	
	let value: KafkaType?
    init(value: OffsetRequestMessage) {
		self.value = value
    }
    
}


struct OffsetRequestMessage: KafkaType {
	
    private var _topics: [TopicalOffsetMessage]
    
    private(set) var replicaId: Int32
    
    init(
        topics: [String:[Int32:(TimeOffset,Int32)]],
        replicaId: ReplicaId = .none
    ) {
        self.replicaId = replicaId.value

        var tempTopics = [TopicalOffsetMessage]()
        
        for (t, p) in topics {
            tempTopics.append(TopicalOffsetMessage(value: t, partitions: p))
        }
        
        _topics = tempTopics
    }
    
    init(data: inout Data) {
        replicaId = Int32(data: &data)
        _topics = [TopicalOffsetMessage](data: &data)
    }
    
    var dataLength: Int {
        return self.replicaId.dataLength +
            self._topics.dataLength
    }
    
	var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self.replicaId.data)
        data.append(self._topics.data)
        return data
    }
}


struct TopicalOffsetMessage: KafkaType {
    private var _partitions: [PartitionedOffsetMessage]
    
    private(set) var topicName: TopicName
    
    init(
        value: String,
        partitions: [Int32: (TimeOffset, Int32)]
    ) {
        topicName = value
        var tempPartitions = [PartitionedOffsetMessage]()
        for (partition, attributes) in partitions {
            tempPartitions.append(
                PartitionedOffsetMessage(
                    value: partition,
                    time: attributes.0,
                    maxNumberOfOffsets: attributes.1
                )
            )
        }
        _partitions = [PartitionedOffsetMessage](tempPartitions)
    }
    
    init(data: inout Data) {
        topicName = String(data: &data)
        _partitions = [PartitionedOffsetMessage](data: &data)
    }
    
    var dataLength: Int {
        return self.topicName.dataLength + self._partitions.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self.topicName.data)
        data.append(self._partitions.data)
        return data
    }
}


struct PartitionedOffsetMessage: KafkaType {
    private var _partition: Int32
    private var _time: Int64
    private var _maxNumberOfOffsets: Int32
    
    var partition: Int32 {
        return _partition
    }
    
    var time: Int64 {
        return _time
    }
    
    var maxNumberOfOffsets: Int32 {
        return _maxNumberOfOffsets
    }
    
    init(value: Int32, time: TimeOffset, maxNumberOfOffsets: Int32) {
        _partition = value
        _time = time.value
        _maxNumberOfOffsets = maxNumberOfOffsets
    }
    
    init(data: inout Data) {
        _partition = Int32(data: &data)
        _time = Int64(data: &data)
        _maxNumberOfOffsets = Int32(data: &data)
    }
    
    var dataLength: Int {
        return self._partition.dataLength +
            self._time.dataLength +
            self._maxNumberOfOffsets.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._partition.data)
        data.append(self._time.data)
        data.append(self._maxNumberOfOffsets.data)
        return data
    }
}


struct OffsetResponse: KafkaResponse {
	
    var values: [TopicalPartitionedOffsets]
    
    init(data: inout Data) {
        values = [TopicalPartitionedOffsets](data: &data)
    }

    var topicalPartitionedOffsets: [TopicalPartitionedOffsets] {
        return values
    }
}


struct TopicalPartitionedOffsets: KafkaType {
    private var _topicName: String
    private var _partitions: [PartitionedOffsets]
    
    var topicName: String {
        return _topicName
    }
    
    var partitionedOffsets: [Int32: PartitionedOffsets] {
        var values = [Int32: PartitionedOffsets]()
        for value in _partitions {
            values[value.partition] = value
        }
        return values
    }
    
    init(data: inout Data) {
        _topicName = String(data: &data)
        _partitions = [PartitionedOffsets](data: &data)
    }
    
    var dataLength: Int {
        return self._topicName.dataLength + self._partitions.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._topicName.data)
        data.append(self._partitions.data)
        return data
    }
}


struct PartitionedOffsets: KafkaType {
    private var _partition: Int32
    private var _errorCode: Int16
    private var _offsets: [Offset]
    
    var partition: Int32 {
        return _partition
    }
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode)
    }
    
    var offsets: [Int64] {
		return _offsets.reversed()
    }

    init(data: inout Data) {
        _partition = Int32(data: &data)
        _errorCode = Int16(data: &data)
        _offsets = [Offset](data: &data)
    }
    
    var dataLength: Int {
        return self._partition.dataLength +
            self._errorCode.dataLength +
            self._offsets.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._partition.data)
        data.append(self._errorCode.data)
        data.append(self._offsets.data)
        return data
    }
}
