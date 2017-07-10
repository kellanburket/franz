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
        partitions: [String:[Int32:Int64]],
        replicaId: ReplicaId = .none,
        minBytes: MinBytes = .one,
        maxWaitTime: Int32 = 500
    ) {
        let message = FetchRequestMessage(
            partitions: partitions,
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

class FetchRequestMessage: KafkaClass {

    private var _replicaId: KafkaInt32
    private var _maxWaitTime: KafkaInt32
    private var _minBytes: KafkaInt32
    private var _topics: KafkaArray<TopicalFetchMessage>

    var replicaId: Int32 {
        return _replicaId.value
    }
    
    var minBytes: Int32 {
        return _minBytes.value
    }
    
    init(
        partitions: [String:[Int32:Int64]],
        replicaId: ReplicaId = .debug,
        minBytes: MinBytes = .one,
        maxWaitTime: Int32 = 500
    ) {
        var tempTopics = [TopicalFetchMessage]()
        
        for (topic, ps) in partitions {
            tempTopics.append(TopicalFetchMessage(value: topic, partitions: ps))
        }
        
        _topics = KafkaArray(values: tempTopics)
        _replicaId = KafkaInt32(value: replicaId.value)
        _minBytes = KafkaInt32(value: minBytes.value)
        _maxWaitTime = KafkaInt32(value: Int32(maxWaitTime))
    }
    
	required init( bytes: inout [UInt8]) {
        _replicaId = KafkaInt32(bytes: &bytes)
        _maxWaitTime = KafkaInt32(bytes: &bytes)
        _minBytes = KafkaInt32(bytes: &bytes)
        _topics = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._replicaId.length +
            self._maxWaitTime.length +
            self._minBytes.length +
            self._topics.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        data.append(self._replicaId.data)
        data.append(self._maxWaitTime.data)
        data.append(self._minBytes.data)
        data.append(self._topics.data)
        
        //print(self.description)
        return data
    }()
    
    lazy var description: String = {
        return "FETCH REQUEST(\(self.length)):\n" +
            "\tREPLICA ID(\(self._replicaId.length)): \(self.replicaId) => \(self._replicaId.data)\n" +
            "\tMAX WAIT TIME(\(self._maxWaitTime.length)): \(self._maxWaitTime.value) => \(self._maxWaitTime.data)\n" +
            "\tMIN BYTES(\(self._minBytes.length)): \(self._minBytes.value) => \(self._minBytes.data)\n" +
            "\tTOPICS(\(self._topics.length)):" +
            self._topics.description
    }()
}

class TopicalFetchMessage: KafkaClass {
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<PartitionedFetchMessage>

    var topicName: String {
        return _topicName.value ?? String()
    }
    
    init(
        value: String,
        partitions: [Int32: Int64]
    ) {
        _topicName = KafkaString(value: value)
        var tempPartitions = [PartitionedFetchMessage]()
        for (partition, offset) in partitions {
            //print("PARTITION(\(partition)), OFFSET(\(offset))")
            tempPartitions.append(
                PartitionedFetchMessage(value: partition, offset: offset)
            )
        }
        _partitions = KafkaArray(values: tempPartitions)
    }
    
	required init( bytes: inout [UInt8]) {
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

class PartitionedFetchMessage: KafkaClass {
    private var _partition: KafkaInt32
    private var _fetchOffset: KafkaInt64
    private var _maxBytes: KafkaInt32 = KafkaInt32(value: 6400)
    
    var partition: Int32 {
        return _partition.value
    }
    
    var offset: Int64 {
        return _fetchOffset.value
    }
    
    var maxBytes: Int32 {
        return _maxBytes.value
    }
    
    init(value: Int32, offset: Int64 = 0) {
        _partition = KafkaInt32(value: value)
        _fetchOffset = KafkaInt64(value: offset)
    }

	required init( bytes: inout [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _fetchOffset = KafkaInt64(bytes: &bytes)
        _maxBytes = KafkaInt32(bytes: &bytes)
    }

    lazy var length: Int = {
        return self._partition.length +
            self._fetchOffset.length +
            self._maxBytes.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        data.append(self._partition.data)
        data.append(self._fetchOffset.data)
        data.append(self._maxBytes.data)
        return data
    }()
    
    lazy var description: String = {
        return "\n\t\t\t----------\n" +
            "\t\t\tPARTITION(\(self._partition.length)): \(self.partition) => \(self._partition.data)\n" +
            "\t\t\tFETCH OFFSET(\(self._fetchOffset.length)): \(self.offset) => \(self._fetchOffset.data)\n" +
            "\t\t\tMAX BYTES(\(self._maxBytes.length)): \(self.maxBytes) => \(self._maxBytes.data)"
        
    }()
}


class FetchResponse: KafkaResponse {
    
    private var _topics: KafkaArray<TopicalFetchResponse>
    
	required init( bytes: inout [UInt8]) {
        _topics = KafkaArray(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    override var description: String {
        return _topics.description
    }
    
    var topics: [TopicalFetchResponse] {
        return _topics.values
    }
}


class TopicalFetchResponse: KafkaClass {
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<PartitionedFetchResponse>
    
    var topicName: String {
        return _topicName.value ?? String()
    }
    
    var partitions: [PartitionedFetchResponse] {
        return _partitions.values
    }
    
	required init( bytes: inout [UInt8]) {
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
        return "\t\tTOPIC NAME(\(self._topicName.length )): " +
                "\(self.topicName) => \(self._topicName.data)\n" +
            "\t\tPARTITIONS(\(self._partitions.length)):" +
                self._partitions.description
    }()
}


class PartitionedFetchResponse: KafkaClass {

    private var _partition: KafkaInt32
    private var _errorCode: KafkaInt16
    private var _highwaterMarkOffset: KafkaInt64
    private var _messageSetSize: KafkaInt32
    private var _messageSet: MessageSet
    
    var partition: Int32 {
        return _partition.value
    }
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode.value)
    }
    
    var offset: Int64 {
        return _highwaterMarkOffset.value
    }
    
    var messages: [Message] {
        return _messageSet.messages
    }
    
	required init( bytes: inout [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _errorCode = KafkaInt16(bytes: &bytes)
        _highwaterMarkOffset = KafkaInt64(bytes: &bytes)
        _messageSetSize = KafkaInt32(bytes: &bytes)
		var messageSetBytes = bytes.slice(0, length: _messageSetSize.value.toInt())
        _messageSet = MessageSet(bytes: &messageSetBytes)
    }
    
    lazy var length: Int = {
        return self._partition.length +
            self._errorCode.length +
            self._highwaterMarkOffset.length +
            self._messageSetSize.length +
            self._messageSet.length
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        data.append(self._partition.data)
        data.append(self._errorCode.data)
        data.append(self._highwaterMarkOffset.data)
        data.append(self._messageSetSize.data)
        data.append(self._messageSet.data)
        return data
    }()
    
    lazy var description: String = {
        return "\n\t\t\t---------\n" +
            "\t\t\tPARTITION: \(self.partition) => \(self._partition.data)\n" +
            "\t\t\tERROR CODE: \(self.error?.code ?? 0)\n" +
            "\t\t\tERROR DESCRIPTION: \(self.error?.description ?? String())\n" +
            "\t\t\tHIGHWATER MARK OFFSET: \(self.offset) => \(self._highwaterMarkOffset.data)\n" +
            "\t\t\tMESSAGE SET SIZE: \(self._messageSetSize.value) => \(self._messageSetSize.data)\n" +
            "\t\t\tMESSAGE SET(\(self._messageSet.length)):" +
            self._messageSet.description
    }()
}
