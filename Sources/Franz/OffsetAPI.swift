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


class OffsetRequestMessage: KafkaClass {
    
    private var _replicaId: KafkaInt32
    private var _topics: KafkaArray<TopicalOffsetMessage>
    
    var replicaId: Int32 {
        return _replicaId.value
    }
    
    init(
        topics: [String:[Int32:(TimeOffset,Int32)]],
        replicaId: ReplicaId = .none
    ) {
        _replicaId = KafkaInt32(value: replicaId.value)

        var tempTopics = [TopicalOffsetMessage]()
        
        for (t, p) in topics {
            tempTopics.append(TopicalOffsetMessage(value: t, partitions: p))
        }
        
        _topics = KafkaArray(values: tempTopics)
    }
    
    required init(bytes: inout [UInt8]) {
        _replicaId = KafkaInt32(bytes: &bytes)
        _topics = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._replicaId.length +
            self._topics.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        data.append(self._replicaId.data)
        data.append(self._topics.data)
        return data
    }()
    
    lazy var description: String = {
        return "FETCH REQUEST(\(self.length)):\n" +
            "\tREPLICA ID(\(self._replicaId.length)): \(self.replicaId) => \(self._replicaId.data)\n" +
            "\tTOPICS(\(self._topics.length)):" +
            self._topics.description
    }()
}


class TopicalOffsetMessage: KafkaClass {
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<PartitionedOffsetMessage>
    
    var topicName: String {
        return _topicName.value ?? String()
    }
    
    init(
        value: String,
        partitions: [Int32: (TimeOffset, Int32)]
    ) {
        _topicName = KafkaString(value: value)
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
        _partitions = KafkaArray(values: tempPartitions)
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
        return "\t\tTOPIC NAME(\(self._topicName.length)): " +
                "\(self.topicName) => \(self._topicName.data)\n" +
            "\t\tPARTITIONS(\(self._partitions.length)):" +
                self._partitions.description
    }()
}


class PartitionedOffsetMessage: KafkaClass {
    private var _partition: KafkaInt32
    private var _time: KafkaInt64
    private var _maxNumberOfOffsets: KafkaInt32
    
    var partition: Int32 {
        return _partition.value
    }
    
    var time: Int64 {
        return _time.value
    }
    
    var maxNumberOfOffsets: Int32 {
        return _maxNumberOfOffsets.value
    }
    
    init(value: Int32, time: TimeOffset, maxNumberOfOffsets: Int32) {
        _partition = KafkaInt32(value: Int32(value))
        _time = KafkaInt64(value: time.value)
        _maxNumberOfOffsets = KafkaInt32(value: maxNumberOfOffsets)
    }
    
    required init(bytes: inout [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _time = KafkaInt64(bytes: &bytes)
        _maxNumberOfOffsets = KafkaInt32(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._partition.length +
            self._time.length +
            self._maxNumberOfOffsets.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        data.append(self._partition.data)
        data.append(self._time.data)
        data.append(self._maxNumberOfOffsets.data)
        return data
    }()
    
    lazy var description: String = {
        return "\n\t\t\t----------\n" +
            "\t\t\tPARTITION(\(self._partition.length)): \(self.partition) => \(self._partition.data)\n" +
            "\t\t\tFETCH OFFSET(\(self._time.length)): \(self.time) => \(self._time.data)\n" +
        "\t\t\tMAX BYTES(\(self._maxNumberOfOffsets.length)): \(self._maxNumberOfOffsets.value) => \(self._maxNumberOfOffsets.data)"
        
    }()
}


class OffsetResponse: KafkaResponse {
    var values: KafkaArray<TopicalPartitionedOffsets>
    
    required init(bytes: inout [UInt8]) {
        values = KafkaArray(bytes: &bytes)
    }
    
    var description: String {
        return values.description
    }

    var topicalPartitionedOffsets: [TopicalPartitionedOffsets] {
        return values.values
    }
}


class TopicalPartitionedOffsets: KafkaClass {
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<PartitionedOffsets>
    
    var topicName: String {
        return _topicName.value ?? String()
    }
    
    var partitionedOffsets: [Int32: PartitionedOffsets] {
        var values = [Int32: PartitionedOffsets]()
        for value in _partitions.values {
            values[value.partition] = value
        }
        return values
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
        return "\t\tTOPIC NAME(\(self._topicName.length )): \(self.topicName) => \(self._topicName.data)\n" +
            "\t\tPARTITIONS(\(self._partitions.length)):" +
            self._partitions.description
    }()
}


class PartitionedOffsets: KafkaClass {
    private var _partition: KafkaInt32
    private var _errorCode: KafkaInt16
    private var _offsets: KafkaArray<KafkaInt64>
    
    var partition: Int32 {
        return _partition.value
    }
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode.value)
    }
    
    var offsets: [Int64] {
        var values = [Int64]()
        for value in _offsets.values {
            values.append(value.value)
        }
        return values.reversed()
    }

    required init(bytes: inout [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _errorCode = KafkaInt16(bytes: &bytes)
        _offsets = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._partition.length +
            self._errorCode.length +
            self._offsets.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        data.append(self._partition.data)
        data.append(self._errorCode.data)
        data.append(self._offsets.data)
        return data
    }()
    
    lazy var description: String = {
        return "\n\t\t\t---------\n" +
            "\t\t\tPARTITION(\(self._partition.length)): \(self.partition) => \(self._partition.data)\n" +
            "\t\t\tERROR CODE(\(self._errorCode.length)): \(self.error?.description ?? String())(\(self.error?.code ?? 0)) => \(self._errorCode.data)\n" +
            "\t\tOFFSETS(\(self._offsets.length)):" +
            self._offsets.description
    }()
}
