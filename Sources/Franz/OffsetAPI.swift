//
//  OffsetAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

class OffsetRequest: KafkaRequest {

    convenience init(
        topic: String,
        partitions: [Int32],
        time: TimeOffset = TimeOffset.latest,
        maxNumberOfOffsets: Int32 = 10,
        replicaId: ReplicaId = .none
    ) {
        var topicValues = [Int32:(TimeOffset, Int32)]()
        for partition in partitions {
            topicValues[partition] = (time, maxNumberOfOffsets)
        }
        
        self.init(
            value: OffsetRequestMessage(
                topics: [topic: topicValues],
                replicaId: replicaId
            )
        )
    }
    
    convenience init(
        topics: [String:[Int32:(TimeOffset,Int32)]],
        replicaId: ReplicaId = .none
    ) {
        self.init(
            value: OffsetRequestMessage(
                topics: topics,
                replicaId: replicaId
            )
        )
    }
    
    init(value: OffsetRequestMessage) {
        super.init(apiKey: ApiKey.offsetRequest, value: value)
    }
    
}


class OffsetRequestMessage: KafkaType {
    
    private var _replicaId: Int32
    private var _topics: KafkaArray<TopicalOffsetMessage>
    
    var replicaId: Int32 {
        return _replicaId
    }
    
    init(
        topics: [String:[Int32:(TimeOffset,Int32)]],
        replicaId: ReplicaId = .none
    ) {
        _replicaId = replicaId.value

        var tempTopics = [TopicalOffsetMessage]()
        
        for (t, p) in topics {
            tempTopics.append(TopicalOffsetMessage(value: t, partitions: p))
        }
        
        _topics = KafkaArray(tempTopics)
    }
    
    required init(data: inout Data) {
        _replicaId = Int32(data: &data)
        _topics = KafkaArray(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._replicaId.dataLength +
            self._topics.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._replicaId.data)
        data.append(self._topics.data)
        return data
    }()
}


class TopicalOffsetMessage: KafkaType {
    private var _topicName: String
    private var _partitions: KafkaArray<PartitionedOffsetMessage>
    
    var topicName: String {
        return _topicName
    }
    
    init(
        value: String,
        partitions: [Int32: (TimeOffset, Int32)]
    ) {
        _topicName = value
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


class PartitionedOffsetMessage: KafkaType {
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
    
    required init(data: inout Data) {
        _partition = Int32(data: &data)
        _time = Int64(data: &data)
        _maxNumberOfOffsets = Int32(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._partition.dataLength +
            self._time.dataLength +
            self._maxNumberOfOffsets.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._partition.data)
        data.append(self._time.data)
        data.append(self._maxNumberOfOffsets.data)
        return data
    }()
}


class OffsetResponse: KafkaResponse {
	
    var values: KafkaArray<TopicalPartitionedOffsets>
    
    required init(data: inout Data) {
        values = KafkaArray(data: &data)
    }

    var topicalPartitionedOffsets: [TopicalPartitionedOffsets] {
        return values.values
    }
}


class TopicalPartitionedOffsets: KafkaType {
    private var _topicName: String
    private var _partitions: KafkaArray<PartitionedOffsets>
    
    var topicName: String {
        return _topicName
    }
    
    var partitionedOffsets: [Int32: PartitionedOffsets] {
        var values = [Int32: PartitionedOffsets]()
        for value in _partitions.values {
            values[value.partition] = value
        }
        return values
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


class PartitionedOffsets: KafkaType {
    private var _partition: Int32
    private var _errorCode: Int16
    private var _offsets: KafkaArray<Offset>
    
    var partition: Int32 {
        return _partition
    }
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode)
    }
    
    var offsets: [Int64] {
        var values = [Int64]()
        for value in _offsets.values {
            values.append(value)
        }
        return values.reversed()
    }

    required init(data: inout Data) {
        _partition = Int32(data: &data)
        _errorCode = Int16(data: &data)
        _offsets = KafkaArray(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._partition.dataLength +
            self._errorCode.dataLength +
            self._offsets.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._partition.data)
        data.append(self._errorCode.data)
        data.append(self._offsets.data)
        return data
    }()
}
