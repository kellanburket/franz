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
    init(values: [String:[Int32:[String]]]) {

        var kafkaTopicalMessageSets = [KafkaTopicalMessageSet]()

        for (topic, partitions) in values {
            var kafkaPartitionedMessageSets = [KafkaPartitionedMessageSet]()
            for (partition, messages) in partitions {
                var messageSetItems = [MessageSetItem]()
                for message in messages {
                    let messageSetItem = MessageSetItem(value: message)
                    messageSetItems.append(messageSetItem)
                }

                let kafkaPartitionedMessageSet = KafkaPartitionedMessageSet(
                    value: MessageSet(values: messageSetItems),
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
            apiKey: ApiKey.ProduceRequest,
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
        self.requestAcks = KafkaInt16(value: RequestAcknowledgement.NoResponse.value)
        self.timeout = KafkaInt32(value: timeout)
    }

    required init(inout bytes: [UInt8]) {
        values = KafkaArray(bytes: &bytes)
        requestAcks = KafkaInt16(bytes: &bytes)
        timeout = KafkaInt32(bytes: &bytes)
    }

    var length: Int {
        return requestAcks.length + timeout.length + values.length
    }
    
    var data: NSData {
        let data = NSMutableData(capacity: length)!

        data.appendData(requestAcks.data)
        data.appendData(timeout.data)
        data.appendData(values.data)
        
        return data
    }

    var description: String {
        return values.description
    }
}

class ProduceResponse: KafkaResponse {

    var values: KafkaArray<TopicalResponse>
    
    required init(inout bytes: [UInt8]) {
        values = KafkaArray(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    override var description: String {
        return values.description
    }
}

class TopicalResponse: KafkaClass {
    
    private var _topicName: KafkaString
    private var _partitions: KafkaArray<PartitionedResponse>
    
    var topicName: String {
        return _topicName.value
    }
    
    var partitions: [PartitionedResponse] {
        var values: [PartitionedResponse] = []

        for partition in _partitions.values {
            values.append(partition)
        }
        
        return values
    }
    
    required init(inout bytes: [UInt8]) {
        _topicName = KafkaString(bytes: &bytes)
        _partitions = KafkaArray(bytes: &bytes)
    }
    
    var length: Int {
        return _topicName.length + _partitions.length
    }
    
    var data: NSData {
        return NSData()
    }

    var description: String {
        return "----------\nTOPICAL RESPONSE:\n" +
            "\tTOPIC NAME: \(topicName)\n" +
            "\tPARTITIONS: \(_partitions.description)"
    }
}

class PartitionedResponse: KafkaClass {
    private var _partition: KafkaInt32
    private var _errorCode: KafkaInt16
    private var _offset: KafkaInt64

    required init(inout bytes: [UInt8]) {
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

    var data: NSData {
        return NSData()
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
    
    required init(inout bytes: [UInt8]) {
        values = KafkaArray(bytes: &bytes)
        topic = KafkaString(bytes: &bytes)
    }
    
    var length: Int {
        return topic.length + values.length
    }
    
    var data: NSData {
        let data = NSMutableData(capacity: Int(length))!
        
        data.appendData(topic.data)
        data.appendData(values.data)
        
        return data
    }
    
    var description: String {
        return "\tTOPIC: \(topic.value) => \(topic.data)\n" +
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
    
    required init(inout bytes: [UInt8]) {
        value = MessageSet(bytes: &bytes)
        partition = KafkaInt32(bytes: &bytes)
    }
    
    var length: Int {
        return partition.length + value.length
    }
    
    var data: NSData {
        let data = NSMutableData(capacity: Int(length))!
        
        data.appendData(partition.data)
        data.appendData(value.data)
        
        return data
    }
    
    var description: String {
        return "\t\tPARTITION: \(partition.value)\n\(value.description)"
    }
}
