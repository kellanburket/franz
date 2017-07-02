//
//  ProduceAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/17/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

import Foundation

class ProduceRequest: KafkaRequest {
    init(values: [String:[Int32:MessageSet]]) {

        var kafkaTopicalMessageSets = [KafkaTopicalMessageSet]()

        for (topic, partitions) in values {
            var kafkaPartitionedMessageSets = [KafkaPartitionedMessageSet]()
            for (partition, messageSet) in partitions {
                let kafkaPartitionedMessageSet = KafkaPartitionedMessageSet(
                    value: messageSet,
                    partition: partition
                )
                
                kafkaPartitionedMessageSets.append(kafkaPartitionedMessageSet)
            }
           
            kafkaTopicalMessageSets.append(
                KafkaTopicalMessageSet(
                    values: kafkaPartitionedMessageSets,
                    topic: topic
                )
            )
        }
        
        super.init(
            apiKey: ApiKey.produceRequest,
            value: ProduceRequestMessage(values: kafkaTopicalMessageSets)
        )
    }
}

class ProduceRequestMessage: KafkaClass {

    var values: KafkaArray<KafkaTopicalMessageSet>
    var requestAcks: KafkaInt16
    var timeout: KafkaInt32
    
    init(
        values: [KafkaTopicalMessageSet],
        timeout: Int32 = Int32(0x05DC)
    ) {
        self.values = KafkaArray(values: values)
        self.requestAcks = KafkaInt16(value: RequestAcknowledgement.noResponse.value)
        self.timeout = KafkaInt32(value: timeout)
    }

    required init(bytes: inout [UInt8]) {
        values = KafkaArray(bytes: &bytes)
        requestAcks = KafkaInt16(bytes: &bytes)
        timeout = KafkaInt32(bytes: &bytes)
    }

    var length: Int {
        return requestAcks.length + timeout.length + values.length
    }
    
    var data: Data {
        var data = Data(capacity: length)

        data.append(requestAcks.data)
        data.append(timeout.data)
        data.append(values.data)
        
        return data
    }

    var description: String {
        return values.description
    }
}

class ProduceResponse: KafkaResponse {

    var values: KafkaArray<TopicalResponse>
    
    required init(bytes: inout [UInt8]) {
        values = KafkaArray(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    override var description: String {
        return values.description
    }
}

class TopicalResponse: KafkaClass {
    
    fileprivate var _topicName: KafkaString
    fileprivate var _partitions: KafkaArray<PartitionedResponse>
    
    var topicName: String {
        return _topicName.value ?? String()
    }
    
    var partitions: [PartitionedResponse] {
        var values: [PartitionedResponse] = []

        for partition in _partitions.values {
            values.append(partition)
        }
        
        return values
    }
    
    required init(bytes: inout [UInt8]) {
        _topicName = KafkaString(bytes: &bytes)
        _partitions = KafkaArray(bytes: &bytes)
    }
    
    var length: Int {
        return _topicName.length + _partitions.length
    }
    
    var data: Data {
        return Data()
    }

    var description: String {
        return "----------\nTOPICAL RESPONSE:\n" +
            "\tTOPIC NAME: \(topicName)\n" +
            "\tPARTITIONS: \(_partitions.description)"
    }
}

class PartitionedResponse: KafkaClass {
    fileprivate var _partition: KafkaInt32
    fileprivate var _errorCode: KafkaInt16
    fileprivate var _offset: KafkaInt64

    required init(bytes: inout [UInt8]) {
        _partition = KafkaInt32(bytes: &bytes)
        _errorCode = KafkaInt16(bytes: &bytes)
        _offset = KafkaInt64(bytes: &bytes)
    }

    var partition: Int32 {
        return _partition.value
    }
    
    var offset: Int64 {
        return _offset.value
    }

    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode.value)
    }
    
    var length: Int {
        return _partition.length + _errorCode.length + _offset.length
    }

    var data: Data {
        return Data()
    }

    var description: String {
        let errorDesscription = error?.description ?? ""
        let errorCode = error?.code ?? 0
        
        return "----------\nPARTITIONED RESPONSE:\n" +
            "\tPARTITION: \(partition)\n" +
            "\tERROR CODE: \(errorCode)\n" +
            "\tERROR DESCRIPTION: \(errorDesscription)\n" +
            "\tOFFSET: \(offset)"
    }
}

class KafkaTopicalMessageSet: KafkaClass {
    var values: KafkaArray<KafkaPartitionedMessageSet>
    var topic: KafkaString
    
    init(values: [KafkaPartitionedMessageSet], topic: String) {
        self.values = KafkaArray(values: values)
        self.topic = KafkaString(value: topic)
    }
    
    required init(bytes: inout [UInt8]) {
        values = KafkaArray(bytes: &bytes)
        topic = KafkaString(bytes: &bytes)
    }
    
    var length: Int {
        return topic.length + values.length
    }
    
    var data: Data {
        var data = Data(capacity: length)
        
        data.append(topic.data)
        data.append(values.data)
        
        return data
    }
    
    var description: String {
        return "\tTOPIC: \(topic.value ?? "nil") => \(topic.data)\n" +
            values.description
    }
}


class KafkaPartitionedMessageSet: KafkaClass {
    var value: MessageSet
    var partition: KafkaInt32
    
    init(value: MessageSet, partition: Int32) {
        self.value = value
        self.partition = KafkaInt32(value: partition)
    }
    
    required init(bytes: inout [UInt8]) {
        value = MessageSet(bytes: &bytes)
        partition = KafkaInt32(bytes: &bytes)
    }
    
    var length: Int {
        return partition.length + value.length
    }
    
    var data: Data {
        var data = Data(capacity: length)
        
        data.append(partition.data)
        data.append(value.data)
        
        return data
    }
    
    var description: String {
        return "\t\tPARTITION: \(partition.value)\n\(value.description)"
    }
}
